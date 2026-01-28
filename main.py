# main.py
"""
Khamenei Index Runner
Runs all signals and aggregates into a single index.
"""

import asyncio
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import clickhouse_connect
import logging
import httpx

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8443))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'telegram')
ALERT_WEBHOOK_URL = os.getenv('ALERT_WEBHOOK_URL', '')

from signals.telegram_signal import TelegramSignal
from signals.rial_signal import RialSignal
from signals.silence_signal import SilenceSignal
from aggregator import KhameneiAggregator


async def send_alert(index, webhook_url: str):
    if not webhook_url:
        return
    
    emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(index.level, "⚪")
    
    payload = {
        "text": f"{emoji} *KHAMENEI INDEX ALERT*",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} Khamenei Index: {index.level}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Score:* {index.score:.1f}/100"},
                    {"type": "mrkdwn", "text": f"*Confidence:* {index.confidence:.0%}"},
                ]
            },
            {
                "type": "section", 
                "text": {"type": "mrkdwn", "text": f"```{json.dumps(index.signals, indent=2)}```"}
            }
        ]
    }
    
    try:
        async with httpx.AsyncClient() as client:
            await client.post(webhook_url, json=payload)
            logger.info(f"Alert sent: {index.level}")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


async def run_index():
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=True,
    )
    logger.info(f"Connected to ClickHouse at {CLICKHOUSE_HOST}")
    
    telegram_signal = TelegramSignal(ch_client, CLICKHOUSE_DATABASE)
    rial_signal = RialSignal()
    silence_signal = SilenceSignal(ch_client, CLICKHOUSE_DATABASE)
    aggregator = KhameneiAggregator(market_deadline="2026-03-31")
    
    last_alerted_level = "GREEN"
    
    logger.info("Calibrating Telegram baseline...")
    baseline = telegram_signal.get_baseline(hours=24)
    telegram_signal.baseline_hits_per_hour = max(baseline, 1.0)
    logger.info(f"Telegram baseline: {baseline:.2f} hits/hour")
    
    logger.info("Starting Khamenei Index monitoring...")
    logger.info("=" * 60)
    
    poll_interval = 60
    
    while True:
        try:
            telegram_result = telegram_signal.fetch()
            rial_result = await rial_signal.fetch()
            silence_result = silence_signal.fetch()
            
            signals = [telegram_result, rial_result, silence_result]
            index = aggregator.aggregate(signals)
            
            print(f"\n{index}")
            print(f"  Telegram: {telegram_result.value:.1f} (critical: {telegram_result.raw_value.get('critical_count', 0)}, routine: {telegram_result.raw_value.get('routine_count', 0)})")
            print(f"  Rial:     {rial_result.value:.1f} (1h change: {rial_result.raw_value.get('change_1h_pct', 0):.2f}%)")
            print(f"  Silence:  {silence_result.value:.1f} (max gap: {silence_result.raw_value.get('max_silence_hours', 0):.1f}h)")
            
            if aggregator.should_alert(index, last_alerted_level):
                logger.warning(f"ALERT TRIGGERED: {last_alerted_level} → {index.level}")
                await send_alert(index, ALERT_WEBHOOK_URL)
                last_alerted_level = index.level
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
        
        await asyncio.sleep(poll_interval)


if __name__ == "__main__":
    try:
        asyncio.run(run_index())
    except KeyboardInterrupt:
        logger.info("Shutting down...")