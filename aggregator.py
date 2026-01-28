from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional
import json

from signals import SignalOutput


@dataclass 
class KhameneiIndex:
    score: float
    confidence: float
    level: str
    signals: dict
    timestamp: datetime
    days_remaining: int
    market_deadline: str
    
    def to_dict(self):
        return {
            "score": round(self.score, 1),
            "confidence": round(self.confidence, 2),
            "level": self.level,
            "signals": self.signals,
            "timestamp": self.timestamp.isoformat(),
            "days_remaining": self.days_remaining,
            "market_deadline": self.market_deadline
        }
    
    def to_json(self):
        return json.dumps(self.to_dict(), indent=2)
    
    def __str__(self):
        emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(self.level, "⚪")
        return f"{emoji} KHAMENEI INDEX: {self.score:.1f}/100 ({self.level}) [conf: {self.confidence:.0%}] | {self.days_remaining} days to {self.market_deadline}"


class KhameneiAggregator:
    def __init__(self, market_deadline: str = "2026-03-31"):
        """
        market_deadline: YYYY-MM-DD format for when the prediction market resolves
        """
        self.market_deadline = market_deadline
        self.deadline_date = datetime.strptime(market_deadline, "%Y-%m-%d")
        
        self.weights = {
            "telegram_velocity": 0.60,
            "rial_crash": 0.40,
            "state_media_silence": 0.00,
        }
        self.thresholds = {"yellow": 35, "red": 65}
        self.history = []
        self.max_history = 60
    
    def get_days_remaining(self):
        now = datetime.utcnow()
        delta = self.deadline_date - now
        return max(0, delta.days)
    
    def get_time_pressure_multiplier(self):
        """
        As deadline approaches, signals become more significant.
        - 60+ days out: 1.0x (baseline)
        - 30 days out: 1.2x
        - 14 days out: 1.5x
        - 7 days out: 2.0x
        - 3 days out: 3.0x
        """
        days = self.get_days_remaining()
        
        if days >= 60:
            return 1.0
        elif days >= 30:
            return 1.0 + (60 - days) / 150  # 1.0 to 1.2
        elif days >= 14:
            return 1.2 + (30 - days) / 53   # 1.2 to 1.5
        elif days >= 7:
            return 1.5 + (14 - days) / 14   # 1.5 to 2.0
        elif days >= 3:
            return 2.0 + (7 - days) / 4     # 2.0 to 3.0
        else:
            return 3.0
    
    def aggregate(self, signals):
        signal_map = {s.name: s for s in signals}
        weighted_sum = 0.0
        weighted_confidence = 0.0
        total_weight = 0.0
        
        for name, weight in self.weights.items():
            if name in signal_map:
                s = signal_map[name]
                effective_weight = weight * s.confidence
                weighted_sum += s.value * effective_weight
                weighted_confidence += s.confidence * weight
                total_weight += effective_weight
        
        if total_weight > 0:
            raw_score = weighted_sum / total_weight
            confidence = weighted_confidence / sum(self.weights.values())
        else:
            raw_score = 0
            confidence = 0
        
        # Apply time pressure multiplier
        time_multiplier = self.get_time_pressure_multiplier()
        score = min(100, raw_score * time_multiplier)
        
        if score >= self.thresholds["red"]:
            level = "RED"
        elif score >= self.thresholds["yellow"]:
            level = "YELLOW"
        else:
            level = "GREEN"
        
        signal_summary = {}
        for s in signals:
            signal_summary[s.name] = {
                "value": round(s.value, 1),
                "confidence": round(s.confidence, 2),
                "raw": s.raw_value
            }
        
        # Add time pressure info
        signal_summary["_time_pressure"] = {
            "multiplier": round(time_multiplier, 2),
            "raw_score": round(raw_score, 1)
        }
        
        index = KhameneiIndex(
            score=score,
            confidence=confidence,
            level=level,
            signals=signal_summary,
            timestamp=datetime.utcnow(),
            days_remaining=self.get_days_remaining(),
            market_deadline=self.market_deadline
        )
        
        self.history.append(index)
        if len(self.history) > self.max_history:
            self.history.pop(0)
        
        return index
    
    def get_rate_of_change(self, minutes=10):
        if len(self.history) < 2:
            return None
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        recent = [h for h in self.history if h.timestamp >= cutoff]
        if len(recent) < 2:
            return None
        return recent[-1].score - recent[0].score
    
    def should_alert(self, current, last_alerted_level):
        if current.level != last_alerted_level:
            return True
        if current.level == "RED":
            return True
        roc = self.get_rate_of_change(10)
        if roc and roc > 20:
            return True
        return False
