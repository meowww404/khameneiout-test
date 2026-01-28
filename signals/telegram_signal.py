"""
Telegram signal with smart relevance scoring.
Not just counting keywords - assesses if message indicates actual threat.
"""

import clickhouse_connect
from datetime import datetime, timedelta
from . import SignalOutput

KHAMENEI_KEYWORDS = [
    'خامنه‌ای',
    'خامنه ای',
    'رهبر انقلاب',
    'رهبر معظم',
    'رهبر جمهوری اسلامی',
    'آیت‌الله خامنه‌ای',
]

CRITICAL_KEYWORDS = [
    # Death / Health
    'مرگ',
    'فوت',
    'درگذشت',
    'بیمارستان',
    'بستری',
    'سکته',
    'حمله قلبی',
    'عمل جراحی',
    'وخیم',
    'بحرانی',
    'حال عمومی',
    'ناتوانی',
    'ناتوانی از انجام وظایف',
    'غیرفعال',

    # Absence / Disappearance
    'غیبت',
    'ناپدید',
    'عدم حضور',

    # Succession / Transition
    'جانشین',
    'جانشینی',
    'انتخاب جانشین',
    'اعلام جانشین',
    'نامزدهای احتمالی',
    'مرحله گذار',
    'شورای انتقالی',
    'تعیین سرپرست',
    'معاون رهبری',

    # Institutional Signals
    'مجلس خبرگان',
    'خبرگان رهبری',
    'جلسه فوق‌العاده خبرگان',
    'جلسه اضطراری',
    'گزارش محرمانه خبرگان',

    # Leaving / Removal
    'استعفا',
    'کناره‌گیری',
    'برکناری',
    'عزل',
    'سلب صلاحیت',
    'لغو اختیار',
    'پایان رهبری',
    'تمام شدن دوره',

    # Funeral / State Signals
    'عزای عمومی',
    'نماز میت',
    'وصیت',
]


# Routine = low threat (normal news)
ROUTINE_KEYWORDS = [
    'دیدار',
    'سخنرانی',
    'پیام',
    'بیانیه',
    'تشکر',
    'قدردانی',
    'سالگرد',
    'مراسم',
    'تبریک',
    'حکم',
    'انتصاب',
]


class TelegramSignal:
    """Smart relevance scoring for Khamenei-related messages."""
    
    def __init__(self, ch_client: clickhouse_connect.driver.Client, database: str):
        self.client = ch_client
        self.database = database
        self.baseline_critical_per_day = 1.0
        
    def fetch(self) -> SignalOutput:
        now = datetime.utcnow()
        window_hours = 24
        since = now - timedelta(hours=window_hours)
        
        khamenei_conditions = " OR ".join([
            f"message_text LIKE '%{kw}%'" for kw in KHAMENEI_KEYWORDS
        ])
        
        query = f"""
        SELECT message_text, message_date, channel_title
        FROM {self.database}.messages
        WHERE message_date >= toDateTime64('{since.strftime('%Y-%m-%d %H:%M:%S')}', 3)
          AND ({khamenei_conditions})
        ORDER BY message_date DESC
        LIMIT 500
        """
        
        try:
            result = self.client.query(query)
            messages = result.result_rows
        except Exception as e:
            return SignalOutput(
                name="telegram_velocity",
                value=0,
                raw_value={"error": str(e)},
                confidence=0,
                timestamp=now
            )
        
        critical_messages = []
        routine_messages = []
        unclear_messages = []
        
        for msg_text, msg_date, channel in messages:
            if not msg_text:
                continue
                
            score = self._score_message(msg_text)
            
            if score >= 3:
                critical_messages.append({
                    "text": msg_text[:100],
                    "score": score,
                    "channel": channel,
                    "date": str(msg_date)
                })
            elif score <= 0:
                routine_messages.append(msg_text[:50])
            else:
                unclear_messages.append(msg_text[:50])
        
        critical_count = len(critical_messages)
        
        if critical_count == 0:
            normalized = 0
        elif critical_count == 1:
            normalized = 25
        elif critical_count == 2:
            normalized = 50
        elif critical_count == 3:
            normalized = 75
        else:
            normalized = 100
        
        critical_channels = len(set(m["channel"] for m in critical_messages))
        if critical_channels >= 2:
            normalized = min(100, normalized * 1.3)
        if critical_channels >= 3:
            normalized = min(100, normalized * 1.5)
        
        total_messages = len(messages)
        if total_messages == 0:
            confidence = 0.3
        elif total_messages < 10:
            confidence = 0.6
        else:
            confidence = 0.9
        
        return SignalOutput(
            name="telegram_velocity",
            value=normalized,
            raw_value={
                "critical_count": critical_count,
                "critical_messages": critical_messages[:5],
                "routine_count": len(routine_messages),
                "unclear_count": len(unclear_messages),
                "total_khamenei_mentions": total_messages,
                "channels_reporting_critical": critical_channels,
                "window_hours": window_hours
            },
            confidence=confidence,
            timestamp=now
        )
    
    def _score_message(self, text: str) -> int:
        """Score a message for threat level. Filters out slogans."""
        if not text:
            return -1
        
        score = 0
        
        has_khamenei = any(kw in text for kw in KHAMENEI_KEYWORDS)
        if not has_khamenei:
            return -1
        
        slogan_patterns = ['مرگ بر خامنه', 'مرگ بر']
        is_slogan = any(pattern in text for pattern in slogan_patterns)
        
        for kw in CRITICAL_KEYWORDS:
            if kw in text:
                if kw == 'مرگ' and is_slogan:
                    continue
                score += 2
        
        for kw in ROUTINE_KEYWORDS:
            if kw in text:
                score -= 1
        
        if any(kw in text for kw in ['بیمارستان', 'بستری', 'سکته']):
            if any(kw in text for kw in ['وخیم', 'بحرانی', 'حال']):
                score += 3
        
        if 'جانشین' in text and 'خبرگان' in text:
            score += 3
        
        if any(kw in text for kw in ['فوت', 'درگذشت']) and not is_slogan:
            score = max(score, 4)
        
        return max(-1, min(5, score))
    
    def get_baseline(self, hours: int = 24) -> float:
        return self.baseline_critical_per_day