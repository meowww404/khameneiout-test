# signals/silence_signal.py
"""
State media silence detection signal.
Monitors regime-affiliated Telegram channels for unusual posting gaps.
"""

import clickhouse_connect
from datetime import datetime, timedelta
from . import SignalOutput


# Regime-affiliated channels
REGIME_CHANNELS = [
    'tasnimnews',
    'FarsNewsAgency', 
    'isnanews',
]


class SilenceSignal:
    """Detects unusual silence from regime media channels."""
    
    def __init__(self, ch_client: clickhouse_connect.driver.Client, database: str):
        self.client = ch_client
        self.database = database
        self.regime_channels = REGIME_CHANNELS
        
    def fetch(self) -> SignalOutput:
        now = datetime.utcnow()
        
        # Check if we're in Tehran business hours (8am-11pm Tehran = UTC+3:30)
        tehran_offset = timedelta(hours=3, minutes=30)
        tehran_time = now + tehran_offset
        tehran_hour = tehran_time.hour
        is_business_hours = 8 <= tehran_hour <= 23
        
        if not is_business_hours:
            return SignalOutput(
                name="state_media_silence",
                value=0,
                raw_value={
                    "reason": "outside_tehran_hours",
                    "tehran_hour": tehran_hour
                },
                confidence=0.3,
                timestamp=now
            )
        
        # Get last post time for each regime channel
        silence_data = {}
        max_silence_hours = 0
        channels_checked = 0
        
        for channel in self.regime_channels:
            query = f"""
            SELECT 
                max(message_date) as last_post
            FROM {self.database}.messages
            WHERE lower(channel_username) = lower('{channel}')
               OR lower(channel_title) LIKE '%{channel.lower()}%'
            """
            
            try:
                result = self.client.query(query)
                if result.result_rows and result.result_rows[0][0]:
                    last_post = result.result_rows[0][0]
                    if isinstance(last_post, str):
                        last_post = datetime.fromisoformat(last_post)
                    hours_silent = (now - last_post).total_seconds() / 3600
                    silence_data[channel] = round(hours_silent, 2)
                    max_silence_hours = max(max_silence_hours, hours_silent)
                    channels_checked += 1
                else:
                    silence_data[channel] = "no_data"
            except Exception as e:
                silence_data[channel] = f"error: {str(e)}"
        
        # Normalize: 3+ hours silence during business hours = 100
        if max_silence_hours <= 1:
            normalized = 0
        else:
            normalized = min(100, (max_silence_hours - 1) * 50)
        
        # Confidence based on channel coverage
        if channels_checked == 0:
            confidence = 0
        else:
            confidence = min(0.8, 0.4 + (channels_checked / len(self.regime_channels)) * 0.4)
        
        return SignalOutput(
            name="state_media_silence",
            value=normalized,
            raw_value={
                "max_silence_hours": round(max_silence_hours, 2),
                "channel_silence": silence_data,
                "tehran_hour": tehran_hour,
                "is_business_hours": is_business_hours
            },
            confidence=confidence,
            timestamp=now
        )