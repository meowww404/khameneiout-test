from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

@dataclass
class SignalOutput:
    name: str
    value: float
    raw_value: dict
    confidence: float
    timestamp: datetime
    
    def to_dict(self):
        return {
            "name": self.name,
            "value": round(self.value, 2),
            "raw": self.raw_value,
            "confidence": round(self.confidence, 2),
            "ts": self.timestamp.isoformat()
        }
