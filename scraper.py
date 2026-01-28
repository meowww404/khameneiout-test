"""
Real-time Telegram Channel Message Scraper with ClickHouse Storage
With duplicate detection - safe to restart without re-inserting old messages
"""

import asyncio
import os
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Message
import clickhouse_connect
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE_NUMBER = os.getenv('TELEGRAM_PHONE')

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8443))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'telegram')

CHANNELS_TO_MONITOR = os.getenv('CHANNELS_TO_MONITOR', '').split(',')
CHANNELS_TO_MONITOR = [c.strip() for c in CHANNELS_TO_MONITOR if c.strip()]


class ClickHouseManager:
    def __init__(self):
        self.client = None
    
    def connect(self):
        self.client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            secure=True,
        )
        logger.info(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    
    def setup_database(self):
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.messages (
            message_id Int64,
            channel_id Int64,
            channel_username String,
            channel_title String,
            sender_id Nullable(Int64),
            sender_username Nullable(String),
            message_text String,
            message_date DateTime64(3),
            edit_date Nullable(DateTime64(3)),
            views Nullable(Int32),
            forwards Nullable(Int32),
            replies Nullable(Int32),
            has_media Bool,
            media_type Nullable(String),
            is_forwarded Bool,
            forward_from_id Nullable(Int64),
            reply_to_msg_id Nullable(Int64),
            raw_json String,
            scraped_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(message_date)
        ORDER BY (channel_id, message_date, message_id)
        """
        self.client.command(create_table_sql)
        logger.info(f"Database and table setup complete in {CLICKHOUSE_DATABASE}")
    
    def message_exists(self, channel_id: int, message_id: int) -> bool:
        """Check if message already exists in database."""
        try:
            result = self.client.query(
                f"SELECT 1 FROM {CLICKHOUSE_DATABASE}.messages WHERE channel_id = {channel_id} AND message_id = {message_id} LIMIT 1"
            )
            return len(result.result_rows) > 0
        except Exception as e:
            logger.error(f"Error checking message existence: {e}")
            return False
    
    def insert_message(self, message_data: dict):
        columns = list(message_data.keys())
        values = [list(message_data.values())]
        
        self.client.insert(
            f"{CLICKHOUSE_DATABASE}.messages",
            values,
            column_names=columns
        )
    
    def close(self):
        if self.client:
            self.client.close()


class TelegramScraper:
    def __init__(self, channels: list):
        self.channels = channels
        self.client = TelegramClient('telegram_scraper_session', API_ID, API_HASH)
        self.db = ClickHouseManager()
        self.channel_entities = {}
        self.stats = {"inserted": 0, "skipped": 0}
    
    async def start(self):
        self.db.connect()
        self.db.setup_database()
        
        await self.client.start(phone=PHONE_NUMBER)
        logger.info("Connected to Telegram")
        
        await self._resolve_channels()
        
        @self.client.on(events.NewMessage(chats=list(self.channel_entities.values())))
        async def handle_new_message(event):
            await self._process_message(event.message)
        
        @self.client.on(events.MessageEdited(chats=list(self.channel_entities.values())))
        async def handle_edited_message(event):
            await self._process_message(event.message, is_edit=True)
        
        logger.info(f"Listening for messages from {len(self.channel_entities)} channels...")
        logger.info(f"Channels: {list(self.channel_entities.keys())}")
        
        await self.client.run_until_disconnected()
    
    async def _resolve_channels(self):
        for channel in self.channels:
            try:
                entity = await self.client.get_entity(channel)
                if isinstance(entity, Channel):
                    self.channel_entities[entity.username or str(entity.id)] = entity
                    logger.info(f"Resolved channel: {entity.title} (@{entity.username})")
                else:
                    logger.warning(f"{channel} is not a channel, skipping")
            except Exception as e:
                logger.error(f"Failed to resolve channel {channel}: {e}")
    
    async def _process_message(self, message: Message, is_edit: bool = False):
        try:
            chat = await message.get_chat()
            
            # Skip if already exists (unless it's an edit)
            if not is_edit and self.db.message_exists(chat.id, message.id):
                self.stats["skipped"] += 1
                return
            
            sender = await message.get_sender() if message.sender_id else None
            
            media_type = None
            if message.media:
                media_type = type(message.media).__name__
            
            message_data = {
                'message_id': message.id,
                'channel_id': chat.id,
                'channel_username': chat.username or '',
                'channel_title': chat.title or '',
                'sender_id': message.sender_id,
                'sender_username': sender.username if sender and hasattr(sender, 'username') else None,
                'message_text': message.text or '',
                'message_date': message.date,
                'edit_date': message.edit_date,
                'views': message.views,
                'forwards': message.forwards,
                'replies': message.replies.replies if message.replies else None,
                'has_media': message.media is not None,
                'media_type': media_type,
                'is_forwarded': message.forward is not None,
                'forward_from_id': message.forward.from_id.channel_id if message.forward and hasattr(message.forward.from_id, 'channel_id') else None,
                'reply_to_msg_id': message.reply_to.reply_to_msg_id if message.reply_to else None,
                'raw_json': message.to_json(),
            }
            
            self.db.insert_message(message_data)
            self.stats["inserted"] += 1
            
            action = "Updated" if is_edit else "New"
            logger.info(f"{action} message in {chat.title}: {message.text[:50] if message.text else '[media]'}...")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    async def fetch_history(self, limit_per_channel: int = 1000):
        """Fetch historical messages from channels (skips duplicates)."""
        for name, entity in self.channel_entities.items():
            logger.info(f"Fetching history from {name}...")
            channel_inserted = 0
            channel_skipped = 0
            
            async for message in self.client.iter_messages(entity, limit=limit_per_channel):
                # Check if exists before processing
                if self.db.message_exists(entity.id, message.id):
                    channel_skipped += 1
                    continue
                await self._process_message(message)
                channel_inserted += 1
            
            logger.info(f"Finished {name}: {channel_inserted} new, {channel_skipped} skipped (already existed)")
        
        logger.info(f"History fetch complete. Total: {self.stats['inserted']} inserted, {self.stats['skipped']} skipped")
    
    def stop(self):
        self.db.close()
        self.client.disconnect()


async def main():
    if not API_ID or not API_HASH:
        logger.error("Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env file")
        return
    
    if not CHANNELS_TO_MONITOR:
        logger.error("Please set CHANNELS_TO_MONITOR in .env file")
        return
    
    scraper = TelegramScraper(CHANNELS_TO_MONITOR)
    
    try:
        # Connect and setup
        scraper.db.connect()
        scraper.db.setup_database()
        await scraper.client.start(phone=PHONE_NUMBER)
        await scraper._resolve_channels()
        
        # Fetch history (safe to run - skips duplicates)
        await scraper.fetch_history(limit_per_channel=1000)
        
        # Start real-time monitoring
        await scraper.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        scraper.stop()


if __name__ == '__main__':
    asyncio.run(main())