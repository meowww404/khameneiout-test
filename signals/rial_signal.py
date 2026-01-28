# signals/rial_signal.py
"""
Rial black market rate signal from Bonbast.
Detects sudden currency crashes indicating insider capital flight.
"""

import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from collections import deque
from . import SignalOutput


class RialSignal:
    """Monitors Rial black market rate for sudden crashes."""
    
    def __init__(self, history_hours: int = 6):
        # Store recent prices: (timestamp, rate)
        self.price_history = deque(maxlen=history_hours * 12)  # 5-min intervals
        self.last_fetch_rate = None
        
    async def fetch(self) -> SignalOutput:
        now = datetime.utcnow()
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    "https://bonbast.com/",
                    headers={
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                        "Accept": "text/html,application/xhtml+xml",
                        "Accept-Language": "en-US,en;q=0.9",
                    }
                )
                r.raise_for_status()
        except Exception as e:
            return SignalOutput(
                name="rial_crash",
                value=0,
                raw_value={"error": f"fetch_failed: {str(e)}"},
                confidence=0,
                timestamp=now
            )
        
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Bonbast structure: look for USD sell rate
        current_rate = None
        
        try:
            # Try multiple selectors (Bonbast changes layout sometimes)
            # Method 1: Direct ID
            usd_sell = soup.select_one('#usd_sell')
            if usd_sell:
                current_rate = int(usd_sell.text.strip().replace(',', ''))
            
            # Method 2: Table row
            if not current_rate:
                for row in soup.select('tr'):
                    cells = row.select('td')
                    if len(cells) >= 3 and 'usd' in row.get('id', '').lower():
                        current_rate = int(cells[1].text.strip().replace(',', ''))
                        break
            
            # Method 3: Data attribute
            if not current_rate:
                usd_elem = soup.select_one('[data-currency="usd"]')
                if usd_elem:
                    sell = usd_elem.select_one('.sell')
                    if sell:
                        current_rate = int(sell.text.strip().replace(',', ''))
                        
        except (ValueError, AttributeError) as e:
            return SignalOutput(
                name="rial_crash",
                value=0,
                raw_value={"error": f"parse_failed: {str(e)}", "html_snippet": r.text[:500]},
                confidence=0,
                timestamp=now
            )
        
        if not current_rate:
            return SignalOutput(
                name="rial_crash",
                value=0,
                raw_value={"error": "rate_not_found", "html_snippet": r.text[:500]},
                confidence=0,
                timestamp=now
            )
        
        # Store current rate
        self.price_history.append((now, current_rate))
        self.last_fetch_rate = current_rate
        
        # Calculate % change vs 1 hour ago
        one_hour_ago = now - timedelta(hours=1)
        past_rate = self._get_rate_at(one_hour_ago)
        
        if past_rate:
            pct_change_1h = ((current_rate - past_rate) / past_rate) * 100
        else:
            pct_change_1h = 0
        
        # Calculate % change vs 4 hours ago
        four_hours_ago = now - timedelta(hours=4)
        past_rate_4h = self._get_rate_at(four_hours_ago)
        pct_change_4h = ((current_rate - past_rate_4h) / past_rate_4h) * 100 if past_rate_4h else 0
        
        # Normalize: 5% crash in 1hr = 100
        if pct_change_1h > 0:
            normalized = min(100, pct_change_1h * 20)
        else:
            normalized = 0
        
        # Confidence based on history depth
        history_minutes = len(self.price_history) * 5
        confidence = min(0.95, 0.5 + (history_minutes / 120) * 0.45)
        
        return SignalOutput(
            name="rial_crash",
            value=normalized,
            raw_value={
                "rate": current_rate,
                "change_1h_pct": round(pct_change_1h, 2),
                "change_4h_pct": round(pct_change_4h, 2),
                "history_depth_minutes": history_minutes
            },
            confidence=confidence,
            timestamp=now
        )
    
    def _get_rate_at(self, target_time: datetime):
        """Find closest historical rate at or before target time."""
        for ts, rate in reversed(self.price_history):
            if ts <= target_time:
                return rate
        return None