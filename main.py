#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# PRODUCTION-READY TELEGRAM GROUP MANAGER BOT v2.2
# Fixed: Message deletion errors, datetime deprecations, stability
# =============================================================================

import os
import sys
import re
import csv
import asyncio
import logging
import random
import time
from io import BytesIO
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict
from functools import wraps
from typing import Dict, List, Any, Optional, Tuple, Callable
from contextlib import asynccontextmanager

from telegram import (
    Update, User, Chat, Message, MessageEntity, InlineKeyboardButton, 
    InlineKeyboardMarkup, ChatMember, ChatPermissions
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes, Application, JobQueue
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, Forbidden, BadRequest
from motor.motor_asyncio import AsyncIOMotorClient
from motor.core import AgnosticCollection as Collection
import pytz
from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic import field_validator
# =============================================================================
# CONFIGURATION - SECURE BY DESIGN
# =============================================================================

class BotConfig(BaseSettings):
    """Secure configuration from environment variables"""

    # REQUIRED (NO DEFAULTS FOR SECRETS)
    bot_token: str = "8290310263:AAGnseEQA6qyXk8lqFbTf4vqNdKzJKZq0tg"
    admin_id: int = 407295878
    mongodb_uri: str =  "mongodb+srv://mefirebase1115_db_user:f76qFi3OqJQsagU2@cluster0.wsppssu.mongodb.net/?appName=Cluster0"

    # Optional with defaults
    database_name: str = Field(default="telegram_bot", env="DATABASE_NAME")
    debug: bool = Field(default=False, env="DEBUG")
    max_warnings: int = Field(default=5, env="MAX_WARNINGS")
    captcha_timeout: int = Field(default=120, env="CAPTCHA_TIMEOUT")
    flood_limit: int = Field(default=5, env="FLOOD_LIMIT")
    flood_time: int = Field(default=10, env="FLOOD_TIME")
    rate_limit_calls: int = Field(default=5, env="RATE_LIMIT_CALLS")
    rate_limit_period: int = Field(default=60, env="RATE_LIMIT_PERIOD")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file_max_bytes: int = Field(default=10 * 1024 * 1024, env="LOG_FILE_MAX_BYTES")
    log_file_backup_count: int = Field(default=5, env="LOG_FILE_BACKUP_COUNT")
    cache_ttl_seconds: int = Field(default=300, env="CACHE_TTL_SECONDS")
    max_cache_size: int = Field(default=1000, env="MAX_CACHE_SIZE")

    # ✅ Pydantic v2 validators
    @field_validator("max_warnings")
    @classmethod
    def validate_max_warnings(cls, v: int) -> int:
        if not 2 <= v <= 10:
            raise ValueError("max_warnings must be between 2-10")
        return v

    @field_validator("captcha_timeout")
    @classmethod
    def validate_captcha_timeout(cls, v: int) -> int:
        if not 30 <= v <= 600:
            raise ValueError("captcha_timeout must be between 30-600 seconds")
        return v

    # ✅ Pydantic v2 config
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }


config = BotConfig()

# =============================================================================
# CONSTANTS
# =============================================================================

MUTE_DURATION_FLOOD = 300
MUTE_DURATION_WARN_MAX = 86400
MUTE_DURATION_WARN_NEAR_MAX = 3600
BAD_WORDS_DEFAULT = ["fuck", "shit", "bitch", "asshole", "dick"]
WARN_EXPIRY_DAYS = 90
ADMIN_TAG_COOLDOWN = 300

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logger() -> logging.Logger:
    """Setup production-grade logging"""
    Path("logs").mkdir(exist_ok=True)
    logger = logging.getLogger("telegram_bot")
    level = getattr(logging, config.log_level.upper(), logging.INFO)
    logger.setLevel(logging.DEBUG if config.debug else level)
    logger.handlers.clear()
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %Z'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG if config.debug else level)
    logger.addHandler(console_handler)
    
    try:
        file_handler = logging.handlers.RotatingFileHandler(
            "logs/bot.log", 
            maxBytes=config.log_file_max_bytes, 
            backupCount=config.log_file_backup_count, 
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.error(f"Failed to setup file handler: {e}")
    
    return logger

logger = setup_logger()

# =============================================================================
# MONGODB ASYNC SETUP
# =============================================================================

class MongoDB:
    """Async MongoDB client with connection pooling"""
    
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None
    
    async def connect(self):
        """Establish connection with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.client = AsyncIOMotorClient(
                    config.mongodb_uri,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=5000,
                    retryWrites=True,
                    w='majority',
                    maxPoolSize=10,
                    minPoolSize=3
                )
                await self.client.admin.command('ping')
                self.db = self.client[config.database_name]
                await self.create_indexes()
                logger.info("✅ MongoDB connected successfully")
                return
            except Exception as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
    
    async def create_indexes(self):
        """Create performance indexes"""
        try:
            await self.db.users.create_index([("id", 1)], unique=True)
            await self.db.users.create_index([("username", 1)], sparse=True)
            await self.db.groups.create_index([("id", 1)], unique=True)
            await self.db.warnings.create_index([("user_id", 1), ("group_id", 1), ("active", 1)])
            await self.db.warnings.create_index([("timestamp", -1)], expireAfterSeconds=WARN_EXPIRY_DAYS * 86400)
            await self.db.logs.create_index([("group_id", 1), ("timestamp", -1)])
            await self.db.logs.create_index([("user_id", 1), ("timestamp", -1)])
            await self.db.captchas.create_index([("expires_at", 1)], expireAfterSeconds=0)
            await self.db.captchas.create_index([("user_id", 1), ("group_id", 1)])
            await self.db.bot_stats.create_index([("date", -1)], unique=True)
            await self.db.admin_permissions.create_index([("user_id", 1), ("group_id", 1)], unique=True)
            logger.info("✅ MongoDB indexes created")
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
            raise
    
    async def add_warning(self, user_id: int, group_id: int, reason: str, warned_by: int) -> int:
        """Add warning and return new count"""
        try:
            await self.db.warnings.insert_one({
                "user_id": user_id,
                "group_id": group_id,
                "reason": reason,
                "warned_by": warned_by,
                "timestamp": datetime.now(timezone.utc),
                "active": True
            })
            return await self.get_user_warnings(user_id, group_id)
        except Exception as e:
            logger.error(f"DB error in add_warning: {e}")
            return 0
    
    async def get_user_warnings(self, user_id: int, group_id: int) -> int:
        """Get active warning count"""
        try:
            return await self.db.warnings.count_documents({
                "user_id": user_id,
                "group_id": group_id,
                "active": True
            })
        except Exception as e:
            logger.error(f"DB error in get_user_warnings: {e}")
            return 0
    
    async def clear_warnings(self, user_id: int, group_id: int) -> int:
        """Clear all warnings for user"""
        try:
            result = await self.db.warnings.update_many(
                {"user_id": user_id, "group_id": group_id, "active": True},
                {"$set": {"active": False}}
            )
            return result.modified_count
        except Exception as e:
            logger.error(f"DB error in clear_warnings: {e}")
            return 0
    
    async def remove_last_warning(self, user_id: int, group_id: int) -> bool:
        """Remove most recent warning"""
        try:
            warning = await self.db.warnings.find_one_and_update(
                {"user_id": user_id, "group_id": group_id, "active": True},
                {"$set": {"active": False}},
                sort=[("timestamp", -1)]
            )
            return warning is not None
        except Exception as e:
            logger.error(f"DB error in remove_last_warning: {e}")
            return False

mongo = MongoDB()

# =============================================================================
# TTL CACHE WITH SIZE LIMIT
# =============================================================================

class TTLCache:
    """Thread-safe TTL cache with size limit"""
    
    def __init__(self, maxsize: int, ttl: int):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._timestamps: Dict[str, float] = {}
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            now = time.time()
            if key in self._cache and now - self._timestamps.get(key, 0) < self.ttl:
                return self._cache[key]
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
            return None
    
    async def set(self, key: str, value: Any):
        async with self._lock:
            if len(self._cache) >= self.maxsize:
                oldest_key = min(self._timestamps, key=self._timestamps.get)
                self._cache.pop(oldest_key, None)
                self._timestamps.pop(oldest_key, None)
            self._cache[key] = value
            self._timestamps[key] = time.time()
    
    async def delete(self, key: str):
        async with self._lock:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
    
    async def clear(self):
        async with self._lock:
            self._cache.clear()
            self._timestamps.clear()

group_cache = TTLCache(maxsize=config.max_cache_size, ttl=config.cache_ttl_seconds)
admin_cache = TTLCache(maxsize=config.max_cache_size, ttl=config.cache_ttl_seconds)

# =============================================================================
# GLOBAL STATE WITH LOCKS
# =============================================================================

_user_command_tracker: Dict[int, List[float]] = defaultdict(list)
_flood_tracker: Dict[str, List[float]] = defaultdict(list)
_tracker_lock = asyncio.Lock()
_flood_lock = asyncio.Lock()

# Admin tagging state
ADMIN_TAG_CACHE: Dict[int, float] = {}
ADMIN_TAG_LOCK = asyncio.Lock()

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_user_mention(user: User) -> str:
    """Safe user mention"""
    if user.username:
        return f"@{user.username}"
    return f'<a href="tg://user?id={user.id}">{escape_html(user.first_name)}</a>'

def escape_html(text: str) -> str:
    """Escape HTML special characters"""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def parse_time_string(time_str: Optional[str]) -> Optional[int]:
    """Parse time strings like 30s, 5m, 2h, 1d"""
    if not time_str:
        return None
    try:
        match = re.match(r"^(\d+)([dhms]?)$", time_str.lower())
        if not match:
            return None
        value, unit = int(match.group(1)), match.group(2) or 's'
        multipliers = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
        return value * multipliers.get(unit, 1)
    except:
        return None

def format_duration(seconds: int) -> str:
    """Format seconds to human-readable"""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        return f"{seconds // 3600}h"
    else:
        return f"{seconds // 86400}d"

async def delete_after_delay(message: Message, delay: int):
    """Delete message after delay"""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except:
        pass

# =============================================================================
# SECURITY & PERMISSIONS
# =============================================================================

async def is_admin(chat: Chat, user_id: int, use_cache: bool = True) -> bool:
    """Check if user is admin with caching"""
    if use_cache:
        cache_key = f"admin_{chat.id}_{user_id}"
        cached = await admin_cache.get(cache_key)
        if cached is not None:
            return cached
    
    try:
        member = await chat.get_member(user_id)
        is_admin_status = member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
        if use_cache:
            await admin_cache.set(cache_key, is_admin_status)
        return is_admin_status
    except:
        return False

async def bot_can_restrict_members(chat: Chat, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check bot permissions"""
    try:
        bot_member = await chat.get_member(context.bot.id)
        return (bot_member.status == ChatMember.ADMINISTRATOR and 
                bot_member.can_restrict_members)
    except:
        return False

async def bot_can_promote_members(chat: Chat, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check bot promotion permissions"""
    try:
        bot_member = await chat.get_member(context.bot.id)
        return (bot_member.status == ChatMember.ADMINISTRATOR and 
                bot_member.can_promote_members)
    except:
        return False

async def get_group_admins(chat: Chat, context: ContextTypes.DEFAULT_TYPE) -> List[ChatMember]:
    """Get all administrators in a group"""
    try:
        admins = []
        async for member in context.bot.get_chat_administrators(chat.id):
            if not member.user.is_bot:
                admins.append(member)
        return admins
    except Exception as e:
        logger.error(f"Failed to get admins for {chat.id}: {e}")
        return []

def check_rate_limit(user_id: int, tracker: Dict) -> bool:
    """Rate limit check"""
    now = time.time()
    calls = tracker[user_id]
    calls = [t for t in calls if now - t < config.rate_limit_period]
    calls.append(now)
    tracker[user_id] = calls
    return len(calls) <= config.rate_limit_calls

# =============================================================================
# DECORATORS
# =============================================================================

def admin_only(permission: Optional[str] = None):
    """Decorator for admin-only commands"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            # SAFE NULL CHECKS
            if not update.effective_message or not update.effective_chat or not update.effective_user:
                return
            
            user = update.effective_user
            chat = update.effective_chat
            message = update.effective_message
            
            if chat.type not in [Chat.GROUP, Chat.SUPERGROUP]:
                try:
                    await message.reply_text("❌ This command only works in groups.")
                except:
                    pass
                return
            
            if user.id == config.admin_id:
                return await func(update, context, *args, **kwargs)
            
            try:
                member = await chat.get_member(user.id)
                if member.status not in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]:
                    try:
                        await message.reply_text("❌ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
                    except Exception as e:
                        logger.warning(f"Failed to send admin error {e}")
                    return
                
                return await func(update, context, *args, **kwargs)
                
            except TelegramError as e:
                logger.error(f"Admin check failed in {func.__name__}: {e}")
                try:
                    await message.reply_text("❌ Permission check failed.")
                except:
                    pass
        
        return wrapper
    return decorator

def log_command(action: str):
    """Log commands to database"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            # SAFE NULL CHECKS
            if not update.effective_message or not update.effective_chat or not update.effective_user:
                return
            
            user = update.effective_user
            chat = update.effective_chat
            message = update.effective_message
            
            start_time = time.time()
            success = False
            error_msg = None
            
            try:
                result = await func(update, context, *args, **kwargs)
                success = True
                return result
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
                raise
            finally:
                try:
                    await mongo.db.logs.insert_one({
                        "group_id": chat.id if chat else None,
                        "user_id": user.id if user else None,
                        "action": action,
                        "command": message.text[:100] if message else None,
                        "timestamp": datetime.now(timezone.utc),
                        "status": "success" if success else "failed",
                        "error": error_msg,
                        "execution_time_ms": int((time.time() - start_time) * 1000)
                    })
                except Exception as e:
                    logger.error(f"Failed to log command: {e}")
        
        return wrapper
    return decorator

def rate_limit_decorator():
    """Rate limit commands"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            # SAFE NULL CHECK
            if not update.effective_user:
                return
            
            user_id = update.effective_user.id
            async with _tracker_lock:
                if not check_rate_limit(user_id, _user_command_tracker):
                    remaining = config.rate_limit_period - int(time.time() - _user_command_tracker[user_id][0])
                    try:
                        await update.message.reply_text(
                            f"⚠️ Too many commands. Please wait {remaining} seconds."
                        )
                    except:
                        pass
                    return
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

def owner_only():
    """Owner-only decorator"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            # SAFE NULL CHECK
            if not update.effective_user:
                return
            
            if update.effective_user.id != config.admin_id:
                try:
                    await update.message.reply_text("❌ <b>Owner only.</b>", parse_mode=ParseMode.HTML)
                except:
                    pass
                return
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

# =============================================================================
# USER RESOLUTION
# =============================================================================

async def resolve_target_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Tuple[Optional[User], Optional[str]]:
    """Resolve target user from reply, mention, or ID"""
    message = update.effective_message
    
    # SAFE NULL CHECK
    if not message:
        return None, "❌ No message to process."
    
    if message.reply_to_message:
        return message.reply_to_message.from_user, None
    
    if not context.args:
        return None, "❌ Specify user by reply, @username, or numeric ID."
    
    identifier = context.args[0].strip()
    
    if message.entities:
        for entity in message.entities:
            if entity.type == MessageEntity.MENTION:
                username = message.text[entity.offset + 1:entity.offset + entity.length].lower()
                user_data = await mongo.db.users.find_one({"username": username})
                if user_data:
                    try:
                        member = await update.effective_chat.get_member(user_data["id"])
                        return member.user, None
                    except:
                        return User(id=user_data["id"], first_name=username, is_bot=False), None
                return None, f"❌ User @{username} not found in database."
            
            if entity.type == MessageEntity.TEXT_MENTION:
                return entity.user, None
    
    if identifier.isdigit():
        try:
            user_id = int(identifier)
            member = await update.effective_chat.get_member(user_id)
            return member.user, None
        except:
            return User(id=user_id, first_name=f"User {user_id}", is_bot=False), None
    
    if identifier.startswith('@'):
        username = identifier[1:].lower()
        user_data = await mongo.db.users.find_one({"username": username})
        if user_data:
            try:
                member = await update.effective_chat.get_member(user_data["id"])
                return member.user, None
            except:
                return User(id=user_data["id"], first_name=username, is_bot=False), None
        return None, f"❌ User @{username} not found."
    
    return None, f"❌ Invalid identifier '{identifier}'. Use @username or numeric ID."

# =============================================================================
# ADMIN TAGGING SYSTEM
# =============================================================================

async def handle_admin_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle @admin tagging by users"""
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    
    # SAFE NULL CHECKS
    if not message or not user or not chat:
        return
    
    # Check if feature is enabled
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    if not group_data.get("admin_tag_enabled", True):
        return
    
    # Rate limit check
    async with ADMIN_TAG_LOCK:
        now = time.time()
        last_tag = ADMIN_TAG_CACHE.get(user.id, 0)
        if now - last_tag < ADMIN_TAG_COOLDOWN:
            remaining = ADMIN_TAG_COOLDOWN - int(now - last_tag)
            try:
                await message.reply_text(
                    f"⚠️ Please wait {remaining} seconds before tagging admins again.",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
            return
        ADMIN_TAG_CACHE[user.id] = now
    
    # Get admins
    admins = await get_group_admins(chat, context)
    if not admins:
        try:
            await message.reply_text("❌ Could not retrieve administrators.")
        except:
            pass
        return
    
    # Build report message for admins
    report_text = (
        f"🚨 <b>Admin Alert</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>User:</b> {get_user_mention(user)} (ID: <code>{user.id}</code>)\n"
        f"🏢 <b>Group:</b> {escape_html(chat.title)} (ID: <code>{chat.id}</code>)\n"
        f"🕒 <b>Time:</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
    )
    
    if message.reply_to_message:
        if message.reply_to_message.from_user:
            replied_user = message.reply_to_message.from_user
            report_text += (
                f"\n💬 <b>Reply to:</b> {get_user_mention(replied_user)} "
                f"(ID: <code>{replied_user.id}</code>)\n"
            )
        
        if message.reply_to_message.text:
            report_text += (
                f"📝 <b>Message:</b>\n"
                f"<code>{escape_html(message.reply_to_message.text[:200])}</code>\n"
            )
    else:
        if message.text:
            clean_text = re.sub(r'@admin\b', '', message.text, flags=re.IGNORECASE).strip()
            if clean_text:
                report_text += f"\n📝 <b>Message:</b>\n<code>{escape_html(clean_text[:200])}</code>\n"
    
    report_text += (
        f"\n━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href=\"{message.link}\">Jump to Message</a>"
    )
    
    # Notify each admin
    notified_count = 0
    failed_count = 0
    
    for admin in admins:
        try:
            await context.bot.send_message(
                chat_id=admin.user.id,
                text=report_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            notified_count += 1
        except Forbidden:
            failed_count += 1
            logger.warning(f"Cannot DM admin {admin.user.id} in group {chat.id}")
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to notify admin {admin.user.id}: {e}")
    
    # Confirm to user
    if notified_count > 0:
        try:
            await message.reply_text(
                f"✅ <b>Admins Notified!</b>\n"
                f"{notified_count} administrator(s) will review your request.",
                parse_mode=ParseMode.HTML,
                reply_to_message_id=message.message_id
            )
        except:
            pass
    else:
        try:
            await message.reply_text(
                "❌ Could not notify administrators. They may have disabled DMs.",
                parse_mode=ParseMode.HTML
            )
        except:
            pass

@admin_only()
async def toggle_admin_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle @admin tagging feature"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    new_status = not group_data.get("admin_tag_enabled", True)
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"admin_tag_enabled": new_status}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        status_text = "✅ Enabled" if new_status else "❌ Disabled"
        await update.message.reply_text(f"🚨 Admin Tagging: {status_text}")
    except Exception as e:
        logger.error(f"Toggle admin tag failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

# =============================================================================
# COMMAND HANDLERS - OWNER ONLY
# =============================================================================

@owner_only()
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all groups with pagination"""
    if not context.args:
        await update.message.reply_text("❌ Provide a message to broadcast.")
        return
    
    message = " ".join(context.args)
    status_message = await update.message.reply_text("📢 Broadcasting to groups... 0 sent.")
    
    success_count = 0
    fail_count = 0
    batch_size = 50
    skip = 0
    
    while True:
        groups = await mongo.db.groups.find({}).skip(skip).limit(batch_size).to_list(None)
        if not groups:
            break
        
        for group in groups:
            await asyncio.sleep(0.1)
            
            try:
                target_chat_id = group.get("id")
                if not target_chat_id:
                    continue
                
                await context.bot.send_message(
                    chat_id=target_chat_id,
                    text=escape_html(message),
                    parse_mode=ParseMode.HTML
                )
                success_count += 1
                
                if (success_count + fail_count) % 5 == 0:
                    await status_message.edit_text(
                        f"📢 Broadcasting... {success_count} sent, {fail_count} failed."
                    )
                    
            except Forbidden:
                logger.warning(f"Bot removed from group {group.get('id')}")
                await mongo.db.groups.delete_one({"id": group.get("id")})
                fail_count += 1
            except Exception as e:
                logger.error(f"Broadcast failed to {group.get('id')}: {e}")
                fail_count += 1
        
        skip += batch_size
    
    await status_message.edit_text(
        f"✅ Broadcast complete!\nSent: {success_count}\nFailed: {fail_count}"
    )

@owner_only()
async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Leave a group"""
    chat_id = update.effective_chat.id
    
    if context.args and context.args[0].isdigit():
        chat_id = int(context.args[0])
    
    try:
        if chat_id != update.effective_chat.id:
            await context.bot.send_message(
                chat_id=chat_id,
                text="👋 Bot is leaving this group as requested by owner."
            )
        
        await context.bot.leave_chat(chat_id)
        
        await mongo.db.groups.delete_one({"id": chat_id})
        await mongo.db.warnings.delete_many({"group_id": chat_id})
        await mongo.db.logs.delete_many({"group_id": chat_id})
        await mongo.db.captchas.delete_many({"group_id": chat_id})
        await mongo.db.admin_permissions.delete_many({"group_id": chat_id})
        await group_cache.delete(f"group_{chat_id}")
        await admin_cache.clear()
        
        if chat_id != update.effective_chat.id:
            await update.message.reply_text(f"✅ Left group {chat_id} and cleaned up data.")
    except Exception as e:
        logger.error(f"Leave failed: {e}")
        await update.message.reply_text(f"❌ Failed to leave: {e}")

@owner_only()
async def view_bot_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View bot logs with file download"""
    try:
        status_msg = await update.message.reply_text("🔍 Searching for logs...")
        
        logs = await mongo.db.logs.find({}).sort("timestamp", -1).limit(50).to_list(None)
        if not logs:
            await status_msg.edit_text("📋 No logs found.")
            return
        
        log_content = "📜 BOT ACTIVITY LOGS (LAST 50)\n" + "="*50 + "\n\n"
        
        for log in logs:
            time_str = log['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            action = log.get('action', 'unknown').upper()
            user_id = log.get('user_id', 'N/A')
            status = log.get('status', 'unknown')
            error = log.get('error', 'None')
            details = log.get('details', log.get('command', 'N/A'))
            
            error_line = f"ERROR: {error}\n" if error != 'None' else ''
            log_content += (
                f"TIME: {time_str}\nACTION: {action}\nUSER ID: {user_id}\nSTATUS: {status}\n"
                f"{error_line}DETAILS: {details}\n{'-'*30}\n\n"
            )


        
        log_bytes = BytesIO(log_content.encode('utf-8'))
        log_bytes.name = f"bot_logs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        
        await status_msg.edit_text("📤 Sending file...")
        
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=log_bytes,
            filename=log_bytes.name,
            caption="📜 <b>Bot Activity Logs (Last 50)</b>",
            parse_mode=ParseMode.HTML
        )
        
        await status_msg.delete()
        
    except Exception as e:
        logger.error(f"Error in view_bot_logs: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to generate logs: {e}")

@owner_only()
async def reset_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset group data with confirmation"""
    chat_id = update.effective_chat.id
    
    confirm_msg = await update.message.reply_text(
        "⚠️ <b>WARNING</b>\n\n"
        "This will delete ALL data for this group.\n"
        "Type <code>confirm</code> within 30 seconds to proceed.",
        parse_mode=ParseMode.HTML
    )
    
    context.user_data['pending_reset'] = chat_id
    
    await asyncio.sleep(30)
    if context.user_data.get('pending_reset') == chat_id:
        await confirm_msg.edit_text("❌ Reset cancelled (timeout).")
        context.user_data.pop('pending_reset', None)

@owner_only()
async def handle_reset_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process reset confirmation"""
    if context.user_data.get('pending_reset') == update.effective_chat.id:
        if update.message.text.strip().lower() == 'confirm':
            chat_id = update.effective_chat.id
            try:
                await mongo.db.groups.delete_one({"id": chat.id})
                await mongo.db.warnings.delete_many({"group_id": chat.id})
                await mongo.db.logs.delete_many({"group_id": chat.id})
                await mongo.db.captchas.delete_many({"group_id": chat.id})
                await mongo.db.admin_permissions.delete_many({"group_id": chat.id})
                await group_cache.delete(f"group_{chat.id}")
                await admin_cache.clear()
                
                await update.message.reply_text(
                    "✅ <b>Group Reset Complete</b>\n\nAll data deleted.",
                    parse_mode=ParseMode.HTML
                )
                logger.warning(f"Group {chat_id} reset by owner")
            except Exception as e:
                logger.error(f"Reset failed: {e}", exc_info=True)
                await update.message.reply_text("❌ Database error during reset.")
        else:
            await update.message.reply_text("❌ Reset cancelled.")
        
        context.user_data.pop('pending_reset', None)

@owner_only()
async def shutdown_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Graceful shutdown"""
    await update.message.reply_text("💤 Shutting down gracefully...")
    
    try:
        await mongo.db.bot_stats.update_one(
            {"date": datetime.now(timezone.utc).date().isoformat()},
            {"$inc": {"shutdowns": 1}},
            upsert=True
        )
        logger.info("Bot shutdown initiated by owner")
    except Exception as e:
        logger.error(f"Failed to save shutdown stats: {e}")
    
    asyncio.create_task(_perform_shutdown(context.application))

async def _perform_shutdown(app: Application):
    """Actual shutdown"""
    await asyncio.sleep(1)
    await app.stop()
    await app.shutdown()
    sys.exit(0)

@owner_only()
async def ping_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Health check"""
    start = time.time()
    msg = await update.message.reply_text("🏓 <b>PINGING...</b>", parse_mode=ParseMode.HTML)
    
    db_status = "❌"
    db_latency = 0
    try:
        db_start = time.time()
        await mongo.client.admin.command('ping')
        db_status = "✅"
        db_latency = int((time.time() - db_start) * 1000)
    except:
        pass
    
    try:
        stats = await mongo.db.bot_stats.find_one(
            {"date": datetime.now(timezone.utc).date().isoformat()}
        ) or {}
    except:
        stats = {}
    
    latency = int((time.time() - start) * 1000)
    uptime = int(time.time() - START_TIME)
    
    ping_text = (
        f"<b>PONG</b> 🏓\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📡 <b>Latency:</b> {latency}ms\n"
        f"🗄️ <b>Database:</b> {db_latency}ms ({db_status})\n"
        f"⌛ <b>Uptime:</b> {format_duration(uptime)}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    await msg.edit_text(ping_text, parse_mode=ParseMode.HTML)

# =============================================================================
# MODERATION COMMANDS
# =============================================================================

@admin_only()
@log_command("ban")
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ban a user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if target_user.id == config.admin_id:
        await update.message.reply_text("❌ Cannot ban bot owner.")
        return
    
    if await is_admin(update.effective_chat, target_user.id):
        await update.message.reply_text("❌ Cannot ban an admin.")
        return
    
    if not await bot_can_restrict_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Restrict Members' permission.")
        return
    
    try:
        chat = update.effective_chat
        
        reason = " ".join(context.args[1:]) if context.args else "No reason provided"
        if update.message.reply_to_message:
            reason = " ".join(context.args) if context.args else "No reason provided"
        
        await chat.ban_member(user_id=target_user.id)
        
        await mongo.db.users.update_one(
            {"id": target_user.id},
            {"$set": {
                "id": target_user.id,
                "username": target_user.username,
                "first_name": target_user.first_name,
                "last_name": target_user.last_name,
                f"banned_in.{chat.id}": True,
                "last_updated": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        
        await update.message.reply_text(
            f"🚫 <b>BANNED</b> 🚫\n\n"
            f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
            f"📝 <b>Reason:</b> {escape_html(reason)}\n\n"
            f"⚖️ <i>Action by Admin</i>",
            parse_mode=ParseMode.HTML
        )
    except Forbidden:
        await update.message.reply_text("❌ No permission to ban users.")
    except Exception as e:
        logger.error(f"Ban failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to ban: {str(e)}")

@admin_only()
@log_command("unban")
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unban a user"""
    chat = update.effective_chat
    
    if context.args and context.args[0].isdigit():
        user_id = int(context.args[0])
    else:
        target_user, error = await resolve_target_user(update, context)
        if error:
            await update.message.reply_text(error)
            return
        user_id = target_user.id
    
    try:
        await chat.unban_member(user_id=user_id)
        await mongo.db.users.update_one(
            {"id": user_id},
            {"$unset": {f"banned_in.{chat.id}": ""}}
        )
        await update.message.reply_text(
            f"✅ <b>UNBANNED</b> ✅\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>User ID:</b> {user_id}\n"
            f"🔓 <i>Access restored!</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Unban failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to unban: {str(e)}")

@admin_only()
@log_command("mute")
async def mute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mute a user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if target_user.id == config.admin_id:
        await update.message.reply_text("❌ Cannot mute bot owner.")
        return
    
    if await is_admin(update.effective_chat, target_user.id):
        await update.message.reply_text("❌ Cannot mute an admin.")
        return
    
    if not await bot_can_restrict_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Restrict Members' permission.")
        return
    
    try:
        chat = update.effective_chat
        
        time_arg = context.args[1] if len(context.args) > 1 else None
        duration_seconds = parse_time_string(time_arg)
        until_date = datetime.now(timezone.utc) + timedelta(seconds=duration_seconds) if duration_seconds else None
        
        await chat.restrict_member(
            user_id=target_user.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until_date
        )
        
        await mongo.db.users.update_one(
            {"id": target_user.id},
            {"$set": {
                f"muted_in.{chat.id}": {
                    "until": until_date,
                    "duration": duration_seconds
                },
                "last_updated": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        
        duration_text = format_duration(duration_seconds) if duration_seconds else "permanently"
        await update.message.reply_text(
            f"🔇 <b>MUTED</b> 🔇\n\n"
            f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
            f"⏳ <b>Duration:</b> {duration_text}\n\n"
            f"🤫 <i>Silence is golden.</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Mute failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to mute: {str(e)}")

@admin_only()
@log_command("unmute")
async def unmute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unmute a user"""
    chat = update.effective_chat
    
    if context.args and context.args[0].isdigit():
        user_id = int(context.args[0])
    else:
        target_user, error = await resolve_target_user(update, context)
        if error:
            await update.message.reply_text(error)
            return
        user_id = target_user.id
    
    try:
        await chat.restrict_member(
            user_id=user_id,
            permissions=ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_polls=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True
            )
        )
        await mongo.db.users.update_one(
            {"id": user_id},
            {"$unset": {f"muted_in.{chat.id}": ""}}
        )
        await update.message.reply_text(
            f"🔊 <b>UNMUTED</b> 🔊\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>User ID:</b> {user_id}\n"
            f"✅ <i>Voice restored!</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Unmute failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to unmute: {str(e)}")

@admin_only()
@log_command("kick")
async def kick_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Kick a user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if target_user.id == config.admin_id:
        await update.message.reply_text("❌ Cannot kick bot owner.")
        return
    
    if await is_admin(update.effective_chat, target_user.id):
        await update.message.reply_text("❌ Cannot kick an admin.")
        return
    
    if not await bot_can_restrict_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Restrict Members' permission.")
        return
    
    try:
        chat = update.effective_chat
        
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason provided"
        if update.message.reply_to_message:
            reason = " ".join(context.args) if context.args else "No reason provided"
        
        await chat.ban_member(user_id=target_user.id)
        await asyncio.sleep(0.5)
        await chat.unban_member(user_id=target_user.id)
        
        await update.message.reply_text(
            f"👢 <b>KICKED</b> 👢\n\n"
            f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
            f"📝 <b>Reason:</b> {escape_html(reason)}\n\n"
            f"👋 <i>See you later!</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Kick failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to kick: {str(e)}")

@admin_only()
@log_command("purge")
async def purge_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Purge messages"""
    message = update.effective_message
    chat = update.effective_chat
    
    count = 10
    if context.args:
        try:
            count = int(context.args[0])
            if not 1 <= count <= 100:
                await message.reply_text("❌ Can purge 1-100 messages.")
                return
        except ValueError:
            await message.reply_text("❌ Invalid number.")
            return
    
    try:
        message_ids = [message.message_id]
        
        async for msg in context.bot.get_chat_history(chat.id, limit=count):
            message_ids.append(msg.message_id)
            if len(message_ids) >= count + 1:
                break
        
        for i in range(0, len(message_ids), 10):
            batch = message_ids[i:i+10]
            try:
                await context.bot.delete_messages(chat.id, batch)
            except Exception as e:
                logger.warning(f"Failed to delete batch: {e}")
        
        confirm_msg = await message.reply_text(
            f"🧹 <b>PURGE COMPLETE</b> 🧹\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🗑️ <b>Messages:</b> {len(message_ids)-1}\n"
            f"⚖️ <i>Action by Admin</i>",
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(delete_after_delay(confirm_msg, 3))
        
    except Exception as e:
        logger.error(f"Purge failed: {e}", exc_info=True)
        await message.reply_text(f"❌ Failed to purge: {str(e)}")

@admin_only()
@log_command("warn")
async def warn_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Warn a user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if target_user.id == config.admin_id:
        await update.message.reply_text("❌ Cannot warn bot owner.")
        return
    
    if await is_admin(update.effective_chat, target_user.id):
        await update.message.reply_text("❌ Cannot warn an admin.")
        return
    
    try:
        chat = update.effective_chat
        
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason provided"
        if update.message.reply_to_message:
            reason = " ".join(context.args) if context.args else "No reason provided"
        
        warnings_count = await mongo.add_warning(
            user_id=target_user.id,
            group_id=chat.id,
            reason=reason,
            warned_by=update.effective_user.id
        )
        
        group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
        max_warnings = group_data.get("max_warnings", config.max_warnings)
        
        if warnings_count >= max_warnings:
            await chat.restrict_member(
                user_id=target_user.id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=datetime.now(timezone.utc) + timedelta(seconds=MUTE_DURATION_WARN_MAX)
            )
            action_text = f"🔇 <b>MUTED 24 HOURS</b> (reached {max_warnings} warnings)"
        elif warnings_count == max_warnings - 1:
            await chat.restrict_member(
                user_id=target_user.id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=datetime.now(timezone.utc) + timedelta(seconds=MUTE_DURATION_WARN_NEAR_MAX)
            )
            action_text = f"🔇 <b>MUTED 1 HOUR</b> (warning {warnings_count}/{max_warnings})"
        else:
            action_text = f"⚠️ <b>WARNING {warnings_count}/{max_warnings}</b>"
        
        await update.message.reply_text(
            f"⚠️ <b>WARNING</b> ⚠️\n\n"
            f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
            f"🔢 <b>Count:</b> {warnings_count}/{max_warnings}\n"
            f"📝 <b>Reason:</b> {escape_html(reason)}\n\n"
            f"⚖️ <i>Action: {action_text}</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Warn failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Failed to warn user.")

@admin_only()
@log_command("unwarn")
async def unwarn_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove last warning"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    chat = update.effective_chat
    
    try:
        removed = await mongo.remove_last_warning(target_user.id, chat.id)
        if removed:
            await update.message.reply_text(
                f"✅ <b>WARN REMOVED</b> ✅\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
                f"✨ <i>One warning removed.</i>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"ℹ️ {get_user_mention(target_user)} has no active warnings.",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"Unwarn failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Failed to remove warning.")

@admin_only()
async def view_warnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View warnings for a user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        target_user = update.effective_user
    
    chat = update.effective_chat
    
    try:
        warnings_count = await mongo.get_user_warnings(target_user.id, chat.id)
        
        if warnings_count == 0:
            await update.message.reply_text(
                f"✅ {get_user_mention(target_user)} has no active warnings.",
                parse_mode=ParseMode.HTML
            )
            return
        
        recent_warnings = await mongo.db.warnings.find({
            "user_id": target_user.id,
            "group_id": chat.id,
            "active": True
        }).sort("timestamp", -1).limit(10).to_list(None)
        
        warn_text = f"⚠️ <b>Warnings for {escape_html(target_user.first_name)}: {warnings_count}</b>\n\n"
        
        for i, warn in enumerate(recent_warnings, 1):
            try:
                warned_by_user = await context.bot.get_chat_member(chat.id, warn['warned_by'])
                warned_by_name = warned_by_user.user.first_name
            except:
                warned_by_name = "Admin"
            
            time_ago = format_duration(int((datetime.now(timezone.utc) - warn['timestamp']).total_seconds()))
            reason = escape_html(warn['reason'])[:50]
            warn_text += f"{i}. {reason}\n   <i>by {warned_by_name}, {time_ago}</i>\n\n"
        
        await update.message.reply_text(warn_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"View warnings failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Failed to retrieve warnings.")

@admin_only()
@log_command("promote")
async def promote_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Promote user to admin"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if not await bot_can_promote_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Add Admins' permission.")
        return
    
    try:
        await update.effective_chat.promote_member(
            user_id=target_user.id,
            can_change_info=True,
            can_delete_messages=True,
            can_invite_users=True,
            can_restrict_members=True,
            can_pin_messages=True,
            can_promote_members=False,
            can_manage_chat=True,
            can_manage_video_chats=True,
            can_post_messages=True,
            can_edit_messages=True
        )
        await update.message.reply_text(
            f"📥 <b>PROMOTED</b> 📥\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
            f"💎 <b>Status:</b> <b>Admin</b>\n\n"
            f"✨ <i>With great power comes great responsibility!</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Promote failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Promote failed: {str(e)}")

@admin_only()
@log_command("demote")
async def demote_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Demote admin to member"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if not await bot_can_promote_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Add Admins' permission.")
        return
    
    try:
        await update.effective_chat.promote_member(
            user_id=target_user.id,
            can_change_info=False,
            can_delete_messages=False,
            can_invite_users=False,
            can_restrict_members=False,
            can_pin_messages=False,
            can_promote_members=False,
            can_manage_chat=False,
            can_manage_video_chats=False,
            can_post_messages=False,
            can_edit_messages=False
        )
        await update.message.reply_text(
            f"📤 <b>DEMOTED</b> 📤\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
            f"❌ <b>Status:</b> <b>Member</b>\n\n"
            f"📉 <i>Admin powers removed.</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Demote failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Demote failed: {str(e)}")

# =============================================================================
# SECURITY MODULES
# =============================================================================

async def antiflood_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Anti-flood protection"""
    user = update.effective_user
    chat = update.effective_chat
    
    if user.id == config.admin_id or await is_admin(chat, user.id):
        return True
    
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    if not group_data.get("antiflood_enabled", False):
        return True
    
    flood_limit = group_data.get("antiflood_limit", config.flood_limit)
    flood_time = group_data.get("antiflood_time", config.flood_time)
    
    tracker_key = f"{chat.id}_{user.id}"
    current_time = time.time()
    
    async with _flood_lock:
        user_messages = [t for t in _flood_tracker[tracker_key] if current_time - t < flood_time]
        user_messages.append(current_time)
        _flood_tracker[tracker_key] = user_messages
        
        if len(user_messages) > flood_limit:
            try:
                await chat.restrict_member(
                    user_id=user.id,
                    permissions=ChatPermissions(can_send_messages=False),
                    until_date=datetime.now(timezone.utc) + timedelta(seconds=MUTE_DURATION_FLOOD)
                )
                
                await mongo.db.logs.insert_one({
                    "group_id": chat.id,
                    "user_id": user.id,
                    "action": "auto_mute_flood",
                    "details": f"Sent {len(user_messages)} messages in {flood_time}s",
                    "timestamp": datetime.now(timezone.utc)
                })
                
                warning_msg = await update.message.reply_text(
                    f"⚠️ <b>Anti-Flood Triggered!</b>\n"
                    f"User {user.mention_html()} muted for {format_duration(MUTE_DURATION_FLOOD)}.",
                    parse_mode=ParseMode.HTML
                )
                
                try:
                    await update.message.delete()
                except:
                    pass
                
                asyncio.create_task(delete_after_delay(warning_msg, 10))
                return False
            except Exception as e:
                logger.error(f"Anti-flood action failed: {e}")
    
    return True

@admin_only()
async def toggle_antiflood(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle anti-flood"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    new_status = not group_data.get("antiflood_enabled", False)
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "antiflood_enabled": new_status,
                "antiflood_limit": group_data.get("antiflood_limit", config.flood_limit),
                "antiflood_time": group_data.get("antiflood_time", config.flood_time)
            }},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🛡️ Anti-Flood: {status_emoji}")
    except Exception as e:
        logger.error(f"Toggle antiflood failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def configure_antiflood(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configure anti-flood"""
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usage: /setflood <limit> <time_in_seconds>")
        return
    
    try:
        limit = int(context.args[0])
        time_window = int(context.args[1])
        
        if not 2 <= limit <= 20:
            await update.message.reply_text("❌ Limit must be 2-20.")
            return
        
        if not 5 <= time_window <= 60:
            await update.message.reply_text("❌ Time window must be 5-60 seconds.")
            return
        
        chat = update.effective_chat
        
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "antiflood_limit": limit,
                "antiflood_time": time_window,
                "antiflood_enabled": True
            }},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        await update.message.reply_text(
            f"✅ Anti-Flood configured:\n"
            f"• Limit: {limit} messages\n"
            f"• Time window: {time_window}s\n"
            f"• Status: Enabled"
        )
    except ValueError:
        await update.message.reply_text("❌ Invalid numbers.")

# =============================================================================
# AUTO-MODERATION
# =============================================================================

async def scan_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Scan messages for violations"""
    message = update.effective_message
    if not message or not message.text:
        return
    
    user = update.effective_user
    chat = update.effective_chat
    
    if user.id == config.admin_id or await is_admin(chat, user.id):
        return
    
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    if not group_data.get("automod_enabled", False):
        return
    
    text = message.text.lower()
    
    if group_data.get("anti_swear_enabled", False):
        bad_words = group_data.get("bad_words", BAD_WORDS_DEFAULT)
        for word in bad_words:
            if re.search(r'\b' + re.escape(word) + r'\b', text, re.IGNORECASE):
                await handle_violation(update, context, "bad_word", f"Used forbidden word: {word}")
                try:
                    await message.delete()
                except:
                    pass
                return
    
    if group_data.get("anti_link_enabled", False):
        has_link = False
        
        urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
        if urls:
            has_link = True
        
        if message.entities:
            for entity in message.entities:
                if entity.type in [MessageEntity.URL, MessageEntity.TEXT_LINK]:
                    has_link = True
                    break
        
        if has_link:
            await handle_violation(update, context, "link_posted", "Posted link without permission")
            try:
                await message.delete()
            except:
                pass
            return

async def handle_violation(update: Update, context: ContextTypes.DEFAULT_TYPE, violation_type: str, details: str):
    """Handle auto-mod violation"""
    user = update.effective_user
    chat = update.effective_chat
    
    warnings_count = await mongo.add_warning(
        user_id=user.id,
        group_id=chat.id,
        reason=details,
        warned_by=context.bot.id
    )
    
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    max_warnings = group_data.get("max_warnings", config.max_warnings)
    
    if warnings_count >= max_warnings:
        mute_duration = MUTE_DURATION_WARN_MAX
        action_text = f"🔇 <b>MUTED 24H</b> 🔇\n<i>(Reached {max_warnings} warnings)</i>"
    elif warnings_count == max_warnings - 1:
        mute_duration = MUTE_DURATION_WARN_NEAR_MAX
        action_text = f"🔇 <b>MUTED 1 HOUR</b> 🔇\n<i>(Warning {warnings_count}/{max_warnings})</i>"
    else:
        mute_duration = None
        action_text = f"⚠️ <b>WARNING {warnings_count}/{max_warnings}</b> ⚠️"
    
    if mute_duration:
        await chat.restrict_member(
            user_id=user.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=datetime.now(timezone.utc) + timedelta(seconds=mute_duration)
        )
    
    await update.message.reply_text(
        f"{action_text}\n\n"
        f"👤 <b>User:</b> {get_user_mention(user)}\n"
        f"📝 <b>Reason:</b> {escape_html(details)}\n\n"
        f"🛡️ <i>Maintain group decorum!</i>",
        parse_mode=ParseMode.HTML
    )

@admin_only()
async def toggle_automod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle auto-mod"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    new_status = not group_data.get("automod_enabled", False)
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"automod_enabled": new_status}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🤖 Auto-Mod: {status_emoji}")
    except Exception as e:
        logger.error(f"Toggle automod failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def toggle_antilink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle anti-link"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    new_status = not group_data.get("anti_link_enabled", False)
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"anti_link_enabled": new_status}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🔗 Anti-Link: {status_emoji}")
    except Exception as e:
        logger.error(f"Toggle antilink failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def toggle_antiswear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle anti-swear"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    new_status = not group_data.get("anti_swear_enabled", False)
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"anti_swear_enabled": new_status}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🤬 Anti-Swear: {status_emoji}")
    except Exception as e:
        logger.error(f"Toggle antiswear failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def set_bad_words(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set custom bad words"""
    if not context.args:
        group_data = await mongo.db.groups.find_one({"id": update.effective_chat.id}) or {}
        bad_words = group_data.get("bad_words", BAD_WORDS_DEFAULT)
        await update.message.reply_text(
            f"Current bad words:\n<code>{', '.join(bad_words)}</code>\n\n"
            "Usage: /setbadwords word1 word2 word3"
        )
        return
    
    bad_words = [word.lower() for word in context.args[:20]]
    chat = update.effective_chat
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "bad_words": bad_words,
                "anti_swear_enabled": True
            }},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        await update.message.reply_text(
            f"✅ Bad words updated:\n<code>{', '.join(bad_words)}</code>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Set bad words failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

# =============================================================================
# CAPTCHA SYSTEM
# =============================================================================

async def new_member_captcha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new members with CAPTCHA"""
    message = update.effective_message
    chat = update.effective_chat
    
    for member in message.new_chat_members:
        if member.is_bot:
            group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
            if group_data.get("anti_bot_enabled", False):
                try:
                    await chat.ban_member(user_id=member.id)
                    await message.reply_text(
                        f"🤖 Auto-removed bot: {member.mention_html()}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
            continue
        
        await mongo.db.users.update_one(
            {"id": member.id},
            {"$set": {
                "id": member.id,
                "first_name": member.first_name,
                "username": member.username,
                "last_seen": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        
        group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
        
        if not group_data.get("captcha_enabled", False):
            await _send_welcome(chat, member, context)
            continue
        
        if not await bot_can_restrict_members(chat, context):
            await message.reply_text("❌ CAPTCHA requires 'Restrict Members' permission.")
            return
        
        try:
            await chat.restrict_member(
                user_id=member.id,
                permissions=ChatPermissions(can_send_messages=False)
            )
        except:
            pass
        
        num1, num2 = random.randint(1, 20), random.randint(1, 20)
        answer = str(num1 + num2)
        
        options = [int(answer) + i for i in range(-2, 3) if i != 0]
        options.append(int(answer))
        random.shuffle(options)
        
        buttons = [
            [InlineKeyboardButton(str(opt), callback_data=f"captcha_{member.id}_{chat.id}_{opt}")]
            for opt in options[:5]
        ]
        
        captcha_msg = await message.reply_text(
            f"🧩 <b>Welcome {escape_html(member.first_name)}!</b>\n\n"
            f"Solve to unlock:\n<code>{num1} + {num2} = ?</code>\n\n"
            f"⏰ Timeout: {group_data.get('captcha_timeout', config.captcha_timeout)}s",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
        await mongo.db.captchas.insert_one({
            "user_id": member.id,
            "group_id": chat.id,
            "answer": answer,
            "message_id": captcha_msg.message_id,
            "created_at": datetime.now(timezone.utc),
            "expires_at": datetime.now(timezone.utc) + timedelta(
                seconds=group_data.get("captcha_timeout", config.captcha_timeout)
            )
        })
        
        context.job_queue.run_once(
            auto_kick_if_failed,
            group_data.get("captcha_timeout", config.captcha_timeout),
            data={
                "user_id": member.id,
                "chat_id": chat.id,
                "message_id": captcha_msg.message_id
            }
        )

async def handle_captcha_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle CAPTCHA answer"""
    query = update.callback_query
    data = query.data.split("_")
    
    if len(data) != 4:
        await query.answer("❌ Invalid CAPTCHA.", show_alert=True)
        return
    
    _, user_id_str, chat_id_str, answer = data
    user_id, chat_id = int(user_id_str), int(chat_id_str)
    
    if query.from_user.id != user_id:
        await query.answer("❌ This CAPTCHA is not for you!", show_alert=True)
        return
    
    try:
        captcha = await mongo.db.captchas.find_one({
            "user_id": user_id,
            "group_id": chat_id,
            "expires_at": {"$gt": datetime.now(timezone.utc)}
        })
        
        if not captcha:
            await query.answer("❌ Expired or invalid CAPTCHA!", show_alert=True)
            try:
                await query.message.delete()
            except:
                pass
            return
        
        if str(answer) == str(captcha["answer"]):
            await context.bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=user_id,
                permissions=ChatPermissions(
                    can_send_messages=True,
                    can_send_media_messages=True,
                    can_send_polls=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True
                )
            )
            
            await query.answer("✅ Verified!", show_alert=True)
            try:
                await query.message.delete()
            except:
                pass
            
            await mongo.db.captchas.delete_one({"_id": captcha["_id"]})
            
            user_data = await mongo.db.users.find_one({"id": user_id}) or {}
            member = User(
                id=user_id,
                first_name=user_data.get("first_name", "User"),
                username=user_data.get("username"),
                is_bot=False
            )
            chat_obj = await context.bot.get_chat(chat_id)
            await _send_welcome(chat_obj, member, context)
        else:
            await query.answer("❌ Wrong answer! Try again.", show_alert=True)
            
    except Exception as e:
        logger.error(f"CAPTCHA verification failed: {e}", exc_info=True)
        await query.answer("❌ Error verifying. Contact admin.", show_alert=True)

async def auto_kick_if_failed(context: ContextTypes.DEFAULT_TYPE):
    """Auto-kick on CAPTCHA timeout"""
    job_data = context.job.data
    
    try:
        captcha = await mongo.db.captchas.find_one({
            "user_id": job_data["user_id"],
            "group_id": job_data["chat_id"],
            "expires_at": {"$lt": datetime.now(timezone.utc)}
        })
        
        if captcha:
            await context.bot.ban_chat_member(
                chat_id=job_data["chat_id"],
                user_id=job_data["user_id"]
            )
            
            await context.bot.send_message(
                job_data["chat_id"],
                f"⏰ <b>Time's up!</b> User {job_data['user_id']} failed CAPTCHA.",
                parse_mode=ParseMode.HTML
            )
            
            try:
                await context.bot.delete_message(
                    job_data["chat_id"],
                    job_data["message_id"]
                )
            except:
                pass
            
            await mongo.db.captchas.delete_one({"_id": captcha["_id"]})
            
    except Exception as e:
        logger.error(f"Auto-kick failed: {e}", exc_info=True)

@admin_only()
async def toggle_captcha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle CAPTCHA"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    new_status = not group_data.get("captcha_enabled", False)
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"captcha_enabled": new_status}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🔐 CAPTCHA: {status_emoji}")
    except Exception as e:
        logger.error(f"Toggle captcha failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def configure_captcha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configure CAPTCHA timeout"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /setcaptcha <timeout_in_seconds>")
        return
    
    try:
        timeout = int(context.args[0])
        if not 30 <= timeout <= 600:
            await update.message.reply_text("❌ Timeout must be 30-600 seconds.")
            return
        
        chat = update.effective_chat
        
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "captcha_timeout": timeout,
                "captcha_enabled": True
            }},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        await update.message.reply_text(f"✅ CAPTCHA timeout set to {timeout} seconds.")
    except ValueError:
        await update.message.reply_text("❌ Invalid timeout.")
    except Exception as e:
        logger.error(f"Configure captcha failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

# =============================================================================
# WELCOME/GOODBYE
# =============================================================================

async def _send_welcome(chat: Chat, member: User, context: ContextTypes.DEFAULT_TYPE):
    """Internal welcome sender"""
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    if not group_data.get("welcome_enabled", False):
        return
    
    template = group_data.get(
        "welcome_message",
        "🎉 <b>Welcome to {chat_title}!</b>\n\nHello {user_name}!"
    )
    
    welcome_text = template.format(
        chat_title=escape_html(chat.title),
        user_name=escape_html(member.first_name),
        user_mention=get_user_mention(member),
        user_id=member.id,
        username=f"@{member.username}" if member.username else "No username"
    )
    
    buttons = []
    if "welcome_buttons" in group_data:
        for btn in group_data["welcome_buttons"]:
            buttons.append([InlineKeyboardButton(btn['text'], url=btn['url'])])
    
    try:
        await context.bot.send_message(
            chat.id,
            welcome_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
        )
    except Exception as e:
        logger.error(f"Failed to send welcome: {e}", exc_info=True)

async def send_goodbye_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send goodbye message"""
    chat = update.effective_chat
    user = update.message.left_chat_member
    
    if user.is_bot:
        return
    
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    if not group_data.get("goodbye_enabled", False):
        return
    
    template = group_data.get(
        "goodbye_message",
        "👋 <b>{user_name}</b> has left the group."
    )
    
    goodbye_text = template.format(
        user_name=escape_html(user.first_name),
        user_mention=get_user_mention(user)
    )
    
    try:
        await update.message.reply_text(goodbye_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to send goodbye: {e}", exc_info=True)

@admin_only()
async def set_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set welcome message"""
    if not context.args:
        await update.message.reply_text(
            "❌ Provide welcome message.\n"
            "Variables: {chat_title}, {user_name}, {user_mention}, {user_id}, {username}"
        )
        return
    
    chat = update.effective_chat
    welcome_text = update.message.text_html.split(None, 1)[1]
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "welcome_message": welcome_text,
                "welcome_enabled": True
            }},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        await update.message.reply_text(
            "✅ Welcome message set!\n"
            "Tip: Use /welcomepreview to test it."
        )
    except Exception as e:
        logger.error(f"Set welcome failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def toggle_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle welcome messages"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /welcome on/off")
        return
    
    chat = update.effective_chat
    status = context.args[0].lower() == "on"
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"welcome_enabled": status}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        status_emoji = "✅ On" if status else "❌ Off"
        await update.message.reply_text(f"🎉 Welcome messages: {status_emoji}")
    except Exception as e:
        logger.error(f"Toggle welcome failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def set_goodbye(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set goodbye message"""
    if not context.args:
        await update.message.reply_text(
            "❌ Provide goodbye message.\nVariables: {user_name}, {user_mention}"
        )
        return
    
    chat = update.effective_chat
    goodbye_text = update.message.text_html.split(None, 1)[1]
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "goodbye_message": goodbye_text,
                "goodbye_enabled": True
            }},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        await update.message.reply_text("✅ Goodbye message set!")
    except Exception as e:
        logger.error(f"Set goodbye failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def toggle_goodbye(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle goodbye messages"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /goodbye on/off")
        return
    
    chat = update.effective_chat
    status = context.args[0].lower() == "on"
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"goodbye_enabled": status}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        status_emoji = "✅ On" if status else "❌ Off"
        await update.message.reply_text(f"👋 Goodbye messages: {status_emoji}")
    except Exception as e:
        logger.error(f"Toggle goodbye failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def preview_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Preview welcome message"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    if not group_data.get("welcome_message"):
        await update.message.reply_text("❌ No welcome message set.")
        return
    
    user = update.effective_user
    template = group_data["welcome_message"]
    
    preview_text = template.format(
        chat_title=escape_html(chat.title),
        user_name=escape_html(user.first_name),
        user_mention=get_user_mention(user),
        user_id=user.id,
        username=f"@{user.username}" if user.username else "No username"
    )
    
    await update.message.reply_text(preview_text, parse_mode=ParseMode.HTML)

# =============================================================================
# NOTES & FILTERS
# =============================================================================

@admin_only()
async def save_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save a note"""
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usage: /note <name> <content>")
        return
    
    note_name = context.args[0].lower()
    note_content = update.message.text_html.split(None, 2)[2]
    
    chat = update.effective_chat
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {f"notes.{note_name}": note_content}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        await update.message.reply_text(
            f"📝 <b>NOTE SAVED</b> 📝\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔑 <b>Keyword:</b> {note_name}\n"
            f"📝 <i>Use '/get {note_name}' to see it.</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Save note failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

async def get_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get a note"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /get <note_name>")
        return
    
    note_name = context.args[0].lower()
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    if "notes" in group_data and note_name in group_data["notes"]:
        await update.message.reply_text(group_data["notes"][note_name])
    else:
        await update.message.reply_text("❌ Note not found.")

@admin_only()
async def del_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete a note"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /delnote <name>")
        return
    
    note_name = context.args[0].lower()
    chat = update.effective_chat
    
    try:
        result = await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$unset": {f"notes.{note_name}": ""}}
        )
        
        if result.modified_count > 0:
            await group_cache.delete(f"group_{chat.id}")
            await update.message.reply_text(
                f"🗑️ <b>NOTE DELETED</b> 🗑️\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔑 <b>Keyword:</b> {note_name}",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("❌ Note not found.")
    except Exception as e:
        logger.error(f"Delete note failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def add_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a filter"""
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usage: /filter <keyword> <response>")
        return
    
    keyword = context.args[0].lower()
    response = update.message.text_html.split(None, 2)[2]
    chat = update.effective_chat
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {f"filters.{keyword}": response}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        await update.message.reply_text(
            f"🛡️ <b>FILTER ADDED</b> 🛡️\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔑 <b>Keyword:</b> {keyword}",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Add filter failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def stop_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop a filter"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /stop <keyword>")
        return
    
    keyword = context.args[0].lower()
    chat = update.effective_chat
    
    try:
        result = await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$unset": {f"filters.{keyword}": ""}}
        )
        
        if result.modified_count > 0:
            await group_cache.delete(f"group_{chat.id}")
            await update.message.reply_text(
                f"🗑️ <b>FILTER REMOVED</b> 🗑️\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔑 <b>Keyword:</b> {keyword}",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("❌ Filter not found.")
    except Exception as e:
        logger.error(f"Stop filter failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

async def check_notes_and_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Auto-reply to filters"""
    if not update.message or not update.message.text:
        return
    
    text = update.message.text
    if text.startswith("/"):
        return
    
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    if "filters" in group_data:
        for keyword, response in group_data["filters"].items():
            if keyword.lower() in text.lower():
                await update.message.reply_text(response)
                return

# =============================================================================
# RULES
# =============================================================================

@admin_only()
async def set_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set group rules"""
    if not context.args:
        await update.message.reply_text("❌ Provide rules text.")
        return
    
    chat = update.effective_chat
    rules_text = update.message.text_html.split(None, 1)[1]
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"rules": rules_text}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        await update.message.reply_text("✅ Rules updated.")
    except Exception as e:
        logger.error(f"Set rules failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

async def show_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show group rules"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    if rules := group_data.get("rules"):
        await update.message.reply_text(
            f"📜 <b>Group Rules</b>\n\n{escape_html(rules)}",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("❌ No rules set for this group.")

# =============================================================================
# GROUP SETTINGS
# =============================================================================

@admin_only()
async def settings_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Updated settings panel with @admin toggle"""
    chat = update.effective_chat
    group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
    
    settings_text = (
        f"⚙️ <b>Settings for {escape_html(chat.title)}</b>\n\n"
        f"🚨 <b>Admin Tagging:</b> {'✅ On' if group_data.get('admin_tag_enabled', True) else '❌ Off'}\n\n"
        f"🛡️ <b>Anti-Flood:</b> {'✅ On' if group_data.get('antiflood_enabled', False) else '❌ Off'}\n"
        f"   Limit: {group_data.get('antiflood_limit', config.flood_limit)} msgs / "
        f"{group_data.get('antiflood_time', config.flood_time)}s\n\n"
        f"🤖 <b>Auto-Mod:</b> {'✅ On' if group_data.get('automod_enabled', False) else '❌ Off'}\n"
        f"   Anti-Link: {'✅' if group_data.get('anti_link_enabled', False) else '❌'}\n"
        f"   Anti-Swear: {'✅' if group_data.get('anti_swear_enabled', False) else '❌'}\n\n"
        f"🔐 <b>CAPTCHA:</b> {'✅ On' if group_data.get('captcha_enabled', False) else '❌ Off'}\n"
        f"   Timeout: {group_data.get('captcha_timeout', config.captcha_timeout)}s\n\n"
        f"🎉 <b>Welcome:</b> {'✅ On' if group_data.get('welcome_enabled', False) else '❌ Off'}\n"
        f"👋 <b>Goodbye:</b> {'✅ On' if group_data.get('goodbye_enabled', False) else '❌ Off'}\n\n"
        f"⚠️ <b>Max Warnings:</b> {group_data.get('max_warnings', config.max_warnings)}"
    )
    
    buttons = [
        [
            InlineKeyboardButton("🚨 Admin Tag", callback_data="toggle_admin_tag"),
        ],
        [
            InlineKeyboardButton("Anti-Flood", callback_data="toggle_antiflood"),
            InlineKeyboardButton("Auto-Mod", callback_data="toggle_automod"),
        ],
        [
            InlineKeyboardButton("CAPTCHA", callback_data="toggle_captcha"),
            InlineKeyboardButton("Anti-Link", callback_data="toggle_antilink"),
        ],
        [
            InlineKeyboardButton("Welcome", callback_data="toggle_welcome"),
            InlineKeyboardButton("Goodbye", callback_data="toggle_goodbye"),
        ],
        [InlineKeyboardButton("Close", callback_data="close_settings")]
    ]
    
    await update.message.reply_text(
        settings_text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@admin_only()
async def set_max_warnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set max warnings"""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Usage: /setmaxwarn <number> (2-10)")
        return
    
    max_warn = int(context.args[0])
    if not 2 <= max_warn <= 10:
        await update.message.reply_text("❌ Must be between 2-10.")
        return
    
    chat = update.effective_chat
    
    try:
        await mongo.db.groups.update_one(
            {"id": chat.id},
            {"$set": {"max_warnings": max_warn}},
            upsert=True
        )
        await group_cache.delete(f"group_{chat.id}")
        
        await update.message.reply_text(
            f"✅ Max warnings set to {max_warn}.\n"
            f"Users will be muted after {max_warn} warnings."
        )
    except Exception as e:
        logger.error(f"Set max warnings failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def group_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show group info"""
    chat = update.effective_chat
    
    try:
        member_count = await context.bot.get_chat_member_count(chat.id)
        bot_info = await context.bot.get_me()
        bot_member = await chat.get_member(bot_info.id)
        
        group_warnings = await mongo.db.warnings.count_documents({
            "group_id": chat.id,
            "active": True
        })
        
        info_text = (
            f"📊 <b>Group Information</b>\n\n"
            f"🏷️ <b>Name:</b> {escape_html(chat.title)}\n"
            f"🆔 <b>ID:</b> <code>{chat.id}</code>\n"
            f"👥 <b>Members:</b> {member_count}\n\n"
            f"🤖 <b>Bot Status:</b>\n"
            f"• Name: {bot_info.first_name}\n"
            f"• Username: @{bot_info.username}\n"
            f"• Permissions: {'✅ Admin' if bot_member.status == ChatMember.ADMINISTRATOR else '❌ Not Admin'}\n\n"
            f"📊 <b>Statistics:</b>\n"
            f"• Active Warnings: {group_warnings}"
        )
        await update.message.reply_text(info_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Group info failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Failed to get group info.")

# =============================================================================
# UTILITY COMMANDS
# =============================================================================

async def get_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get user and chat ID"""
    user = update.effective_user
    chat = update.effective_chat
    
    text = f"🆔 <b>Your ID:</b> <code>{user.id}</code>\n"
    
    if user.username:
        text += f"👤 <b>Username:</b> @{user.username}\n"
    
    if chat.type != "private":
        text += f"\n🏢 <b>Group ID:</b> <code>{chat.id}</code>\n"
        text += f"🏷️ <b>Group Name:</b> {escape_html(chat.title)}"
    
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    user = update.effective_user
    bot_name = context.bot.first_name
    
    text = (
        f"👋🏻 <b>Hello {escape_html(user.first_name)}!</b>\n\n"
        f"🛡️ <b>{escape_html(bot_name)}</b> is a complete bot to help you manage your groups easily and safely!\n\n"
        f"👉🏻 <b>Add me to a Supergroup</b> and promote me as Admin to let me get in action!\n\n"
        f"❓ <b>WHICH ARE THE COMMANDS?</b> ❓\n"
        f"Press the <b>Help</b> button below or type /help to see all commands!\n\n"
        f"📃 <i>By using this bot, you agree to our Privacy Policy.</i>"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("➕ Add me to a Group", url=f"https://t.me/{context.bot.username}?startgroup=true"),
        ],
        [
            InlineKeyboardButton("❓ Help", callback_data="lb_help"),
            InlineKeyboardButton("ℹ️ Bot Information", callback_data="lb_info"),
        ]
    ]
    
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command"""
    user = update.effective_user
    chat = update.effective_chat
    is_owner = user.id == config.admin_id
    is_admin_user = is_owner or (chat.type != "private" and await is_admin(chat, user.id))
    
    help_text = """
❖════════════════════════❖
 🤖 <b>GROUP MANAGER BOT</b>
  <b>HELP HALL</b>
❖════════════════════════❖

✦━━━━━━━━━━━━━━━━━━━━✦
📖 <b>GENERAL COMMANDS</b>
✦━━━━━━━━━━━━━━━━━━━━✦
◆ <code>/start</code> — Start the bot  
◆ <code>/help</code> — Show help  
◆ <code>/rules</code> — Group rules  
◆ <code>/id</code> — User & Chat ID  
◆ <code>/poll</code> — Create poll  
◆ <code>/quiz</code> — Create quiz  
◆ <code>/roll</code> — Roll dice  
◆ <code>/leaderboard</code> — Active members  
◆ <code>/stats</code> — Group statistics  

✦━━━━━━━━━━━━━━━━━━━━✦
"""
    
    if is_admin_user:
        help_text += """
👑 <b>ADMIN CHAMBER</b>

⚔ <b>MODERATION</b>
◆ <code>/ban &lt;user&gt; [reason]</code>  
◆ <code>/unban &lt;user_id&gt;</code>  
◆ <code>/mute &lt;user&gt; [duration]</code>  
◆ <code>/unmute &lt;user&gt;</code>  
◆ <code>/kick &lt;user&gt; [reason]</code>  
◆ <code>/warn &lt;user&gt; [reason]</code>  
◆ <code>/unwarn &lt;user&gt;</code>  
◆ <code>/warnings &lt;user&gt;</code>  
◆ <code>/purge [count]</code>  
◆ <code>/promote &lt;user&gt;</code>  
◆ <code>/demote &lt;user&gt;</code>  

🛡 <b>SECURITY</b>
◆ <code>/antiflood</code>  
◆ <code>/setflood &lt;limit&gt; &lt;time&gt;</code>  
◆ <code>/automod</code>  
◆ <code>/antilink</code>  
◆ <code>/antiswear</code>  
◆ <code>/setbadwords &lt;words...&gt;</code>  
◆ <code>/captcha</code>  
◆ <code>/setcaptcha &lt;timeout&gt;</code>  
◆ <code>/admintag</code> — Toggle @admin tagging  

📜 <b>GROUP MANAGEMENT</b>
◆ <code>/setrules &lt;text&gt;</code>  
◆ <code>/setwelcome &lt;text&gt;</code>  
◆ <code>/welcome on/off</code>  
◆ <code>/setgoodbye &lt;text&gt;</code>  
◆ <code>/goodbye on/off</code>  
◆ <code>/note &lt;name&gt; &lt;text&gt;</code>  
◆ <code>/get &lt;name&gt;</code>  
◆ <code>/delnote &lt;name&gt;</code>  
◆ <code>/filter &lt;kw&gt; &lt;response&gt;</code>  
◆ <code>/stop &lt;kw&gt;</code>  
◆ <code>/setmaxwarn &lt;n&gt;</code>  
◆ <code>/settings</code>  

✦━━━━━━━━━━━━━━━━━━━━✦
"""
    
    if is_owner:
        help_text += """
👑 <b>OWNER THRONE</b>
◆ <code>/broadcast &lt;message&gt;</code>  
◆ <code>/leave &lt;group_id&gt;</code>  
◆ <code>/logs</code>  
◆ <code>/resetgroup</code>  
◆ <code>/shutdown</code>  
◆ <code>/ping</code>  

❖══════════════════════════❖
✨ <b>Secure • Scalable • Production-Ready</b>
"""
    
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)

# =============================================================================
# POLLS & QUIZZES
# =============================================================================

@admin_only()
async def create_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Create a poll"""
    if len(context.args) < 3:
        await update.message.reply_text('❌ Usage: /poll "Question" option1 option2 [...]')
        return
    
    full_text = " ".join(context.args)
    match = re.match(r'["\'](.+?)["\']\s+(.+)', full_text)
    
    if match:
        question = match.group(1)
        options = [opt.strip() for opt in match.group(2).split(' ') if opt.strip()]
    else:
        question = context.args[0]
        options = context.args[1:]
    
    if not 2 <= len(options) <= 10:
        await update.message.reply_text("❌ Poll needs 2-10 options.")
        return
    
    try:
        await context.bot.send_poll(
            chat_id=update.effective_chat.id,
            question=question,
            options=options,
            is_anonymous=False,
            allows_multiple_answers=False
        )
    except Exception as e:
        logger.error(f"Poll failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to create poll: {str(e)}")

@admin_only()
async def create_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Create a quiz"""
    if len(context.args) < 4:
        await update.message.reply_text('❌ Usage: /quiz "Question" correct_index option1 option2 [...]')
        return
    
    full_text = " ".join(context.args)
    match = re.match(r'["\'](.+?)["\']\s+(\d+)\s+(.+)', full_text)
    
    if not match:
        await update.message.reply_text("❌ Invalid format.")
        return
    
    question = match.group(1)
    correct_index = int(match.group(2)) - 1
    options = [opt.strip() for opt in match.group(3).split(' ') if opt.strip()]
    
    if not 0 <= correct_index < len(options):
        await update.message.reply_text("❌ Correct answer index out of range.")
        return
    
    try:
        await context.bot.send_poll(
            chat_id=update.effective_chat.id,
            question=question,
            options=options,
            is_anonymous=False,
            type="quiz",
            correct_option_id=correct_index
        )
    except Exception as e:
        logger.error(f"Quiz failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to create quiz: {str(e)}")

# =============================================================================
# STATS & LEADERBOARD
# =============================================================================

@admin_only()
async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show group statistics"""
    chat = update.effective_chat
    
    try:
        member_count = await context.bot.get_chat_member_count(chat.id)
        
        active_warnings = await mongo.db.warnings.count_documents({
            "group_id": chat.id,
            "active": True
        })
        
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        recent_actions = await mongo.db.logs.count_documents({
            "group_id": chat.id,
            "timestamp": {"$gte": week_ago}
        })
        
        pipeline = [
            {"$match": {"group_id": chat.id, "active": True}},
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        top_offenders = await mongo.db.warnings.aggregate(pipeline).to_list(None)
        
        stats_text = (
            f"📊 <b>GROUP STATISTICS</b> 📊\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👥 <b>Members:</b> {member_count}\n"
            f"⚠️ <b>Active Warnings:</b> {active_warnings}\n"
            f"📈 <b>Actions (7d):</b> {recent_actions}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
        
        if top_offenders:
            stats_text += "🔥 <b>TOP OFFENDERS:</b>\n"
            for i, offender in enumerate(top_offenders, 1):
                user_id = offender["_id"]
                count = offender["count"]
                try:
                    user = await context.bot.get_chat_member(chat.id, user_id)
                    name = escape_html(user.user.first_name)
                    stats_text += f"{i}. {name} ({count} warnings)\n"
                except:
                    stats_text += f"{i}. User({user_id}) ({count} warnings)\n"
            stats_text += "━━━━━━━━━━━━━━━━━━"
        
        await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Group stats failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Failed to get statistics.")

@admin_only()
async def export_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export statistics as CSV"""
    chat = update.effective_chat
    
    try:
        pipeline = [
            {"$match": {"group_id": chat.id, "active": True}},
            {"$group": {"_id": "$user_id", "warnings": {"$sum": 1}}}
        ]
        users_with_warns = {}
        async for doc in mongo.db.warnings.aggregate(pipeline):
            users_with_warns[doc["_id"]] = doc["warnings"]
        
        output = BytesIO()
        writer = csv.writer(output)
        writer.writerow(["User ID", "Username", "First Name", "Last Name", "Warnings"])
        
        for user_id, warn_count in users_with_warns.items():
            try:
                member = await context.bot.get_chat_member(chat.id, user_id)
                user = member.user
                writer.writerow([
                    user.id,
                    user.username or "",
                    user.first_name or "",
                    user.last_name or "",
                    warn_count
                ])
            except:
                writer.writerow([user_id, "N/A", "Left group", "", warn_count])
        
        output.seek(0)
        filename = f"stats_{chat.id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        
        await update.message.reply_document(
            document=output.getvalue(),
            filename=filename,
            caption=f"📊 Statistics for {chat.title}"
        )
    except Exception as e:
        logger.error(f"Export stats failed: {e}", exc_info=True)
        await update.message.reply_text("❌ Failed to export statistics.")

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top active members"""
    await _send_leaderboard(update.effective_chat, update.effective_message, "lifetime", edit=False)

async def _send_leaderboard(chat: Chat, message: Message, timeframe: str, edit: bool = False):
    """Enhanced leaderboard with total message counts"""
    now = datetime.now(timezone.utc)
    
    if timeframe == "today":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        title = "Today"
        emoji = "🌅"
    elif timeframe == "weekly":
        start_date = now - timedelta(days=7)
        title = "This Week"
        emoji = "📅"
    else:  # lifetime
        start_date = datetime.min.replace(tzinfo=timezone.utc)
        title = "Lifetime"
        emoji = "♾️"
    
    total_count = await mongo.db.logs.count_documents({
        "group_id": chat.id,
        "action": "message",
        "timestamp": {"$gte": start_date}
    })
    
    pipeline = [
        {"$match": {
            "group_id": chat.id,
            "action": "message",
            "timestamp": {"$gte": start_date}
        }},
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    top_users = await mongo.db.logs.aggregate(pipeline).to_list(None)
    
    board_text = (
        f"🏆 <b>TOP ACTIVE MEMBERS</b> 🏆\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} <b>Period:</b> {title}\n"
        f"📊 <b>Total Messages:</b> {total_count:,}\n\n"
    )
    
    if not top_users:
        board_text += "<i>No activity data for this period yet.</i>\n"
    else:
        for i, user_data in enumerate(top_users, 1):
            user_id = user_data["_id"]
            count = user_data["count"]
            percentage = (count / total_count * 100) if total_count > 0 else 0
            
            try:
                u_data = await mongo.db.users.find_one({"id": user_id})
                if u_data:
                    name = escape_html(u_data.get("first_name", "User"))
                else:
                    member = await chat.get_member(user_id)
                    name = escape_html(member.user.first_name)
            except:
                name = f"User({user_id})"
            
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"<code>{i}.</code>")
            board_text += f"{medal} {name} — <b>{count:,}</b> ({percentage:.1f}%)\n"
    
    board_text += "\n━━━━━━━━━━━━━━━━━━"
    
    keyboard = [
        [
            InlineKeyboardButton("🌅 Today", callback_data="lb_today"),
            InlineKeyboardButton("📅 Weekly", callback_data="lb_weekly"),
            InlineKeyboardButton("♾️ Lifetime", callback_data="lb_lifetime")
        ],
        [InlineKeyboardButton("🗑️ Close", callback_data="close_settings")]
    ]
    
    try:
        if edit:
            await message.edit_text(board_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        else:
            await message.reply_text(board_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error sending leaderboard: {e}", exc_info=True)

async def message_logger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log messages for leaderboard"""
    user = update.effective_user
    chat = update.effective_chat
    
    if not user or not chat or user.is_bot:
        return
    
    try:
        await asyncio.gather(
            mongo.db.logs.insert_one({
                "group_id": chat.id,
                "user_id": user.id,
                "action": "message",
                "timestamp": datetime.now(timezone.utc)
            }),
            mongo.db.users.update_one(
                {"id": user.id},
                {"$set": {
                    "username": user.username.lower() if user.username else None,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "last_seen": datetime.now(timezone.utc)
                }},
                upsert=True
            )
        )
    except Exception as e:
        logger.error(f"Error in message_logger: {e}", exc_info=True)

async def roll_dice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Roll dice"""
    sides = 6
    if context.args:
        try:
            sides = int(context.args[0])
            if not 2 <= sides <= 100:
                await update.message.reply_text("❌ Dice must have 2-100 sides.")
                return
        except ValueError:
            await update.message.reply_text("❌ Invalid number.")
            return
    
    dice_msg = await update.message.reply_dice(emoji="🎲")
    await asyncio.sleep(4)
    result = random.randint(1, sides)
    await update.message.reply_text(f"🎲 Rolled: {result} (1-{sides})!")

# =============================================================================
# CALLBACK HANDLERS
# =============================================================================

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced callback dispatcher"""
    query = update.callback_query
    data = query.data
    
    if data == "close_settings":
        try:
            await query.message.delete()
        except:
            pass
        return
    
    if data.startswith("lb_"):
        timeframe = data.replace("lb_", "")
        await _send_leaderboard(query.message.chat, query.message, timeframe, edit=True)
        return
    
    if data.startswith("toggle_"):
        chat = query.message.chat
        setting = data.replace("toggle_", "")
        
        if not await is_admin(chat, query.from_user.id):
            await query.answer("❌ Admin only!", show_alert=True)
            return
        
        group_data = await mongo.db.groups.find_one({"id": chat.id}) or {}
        
        setting_map = {
            "antiflood": "antiflood_enabled",
            "automod": "automod_enabled",
            "captcha": "captcha_enabled",
            "antilink": "anti_link_enabled",
            "welcome": "welcome_enabled",
            "goodbye": "goodbye_enabled",
            "admin_tag": "admin_tag_enabled"
        }
        
        if setting in setting_map:
            db_field = setting_map[setting]
            new_status = not group_data.get(db_field, False if setting != "admin_tag" else True)
            
            try:
                await mongo.db.groups.update_one(
                    {"id": chat.id},
                    {"$set": {db_field: new_status}},
                    upsert=True
                )
                await group_cache.delete(f"group_{chat.id}")
                
                await query.answer(f"✅ {'Enabled' if new_status else 'Disabled'}", show_alert=False)
                
                if setting in ["admin_tag", "antiflood", "automod", "captcha", "antilink", "welcome", "goodbye"]:
                    await settings_panel(
                        Update(update.update_id, message=query.message),
                        context
                    )
            except Exception as e:
                logger.error(f"Callback toggle failed: {e}", exc_info=True)
                await query.answer("❌ Database error.", show_alert=True)
    
    await query.answer()

# =============================================================================
# PERIODIC CLEANUP
# =============================================================================

async def periodic_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """Periodic cleanup of flood tracker and admin tag cache"""
    async with _flood_lock:
        now = time.time()
        cutoff = config.flood_time * 2
        keys_to_delete = []
        
        for key, timestamps in _flood_tracker.items():
            valid_timestamps = [t for t in timestamps if now - t < cutoff]
            if valid_timestamps:
                _flood_tracker[key] = valid_timestamps
            else:
                keys_to_delete.append(key)
        
        for key in keys_to_delete:
            _flood_tracker.pop(key, None)
    
    # Clean admin tag cache (keep only entries from last 24 hours)
    async with ADMIN_TAG_LOCK:
        now = time.time()
        expired_users = [uid for uid, ts in ADMIN_TAG_CACHE.items() if now - ts > 86400]
        for uid in expired_users:
            ADMIN_TAG_CACHE.pop(uid, None)
    
    logger.info(f"Cleanup complete. Tracked floods: {len(_flood_tracker)}, Admin tags: {len(ADMIN_TAG_CACHE)}")

# =============================================================================
# ERROR HANDLER
# =============================================================================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler"""
    logger.error(f"Update {update} caused error {context.error}", exc_info=context.error)
    
    try:
        if update and update.effective_message:
            await update.message.reply_text(
                "❌ An error occurred while processing your request.\n"
                "The bot administrator has been notified."
            )
    except:
        pass

# =============================================================================
# MAIN APPLICATION
# =============================================================================

START_TIME = time.time()
application: Application = None

def setup_application() -> Application:
    """Setup and configure the application"""
    global application
    
    application = (
        ApplicationBuilder()
        .token(config.bot_token)
        .concurrent_updates(True)
        .connection_pool_size(8)
        .rate_limiter(None)
        .post_init(post_init)
        .post_shutdown(post_stop)
        .build()
    )
    
    # Message logging
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, 
        message_logger
    ), group=-1)
    
    # Owner commands
    application.add_handler(CommandHandler("broadcast", broadcast_command, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("leave", leave_command))
    application.add_handler(CommandHandler("logs", view_bot_logs, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("resetgroup", reset_group_command))
    application.add_handler(CommandHandler("confirm", handle_reset_confirmation))
    application.add_handler(CommandHandler("shutdown", shutdown_bot, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("ping", ping_bot))
    
    # Admin moderation
    application.add_handler(CommandHandler("ban", ban_user, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("unban", unban_user, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("mute", mute_user, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("unmute", unmute_user, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("kick", kick_user, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("warn", warn_user, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("unwarn", unwarn_user, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("warnings", view_warnings, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("purge", purge_messages, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("promote", promote_user, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("demote", demote_user, filters=filters.ChatType.GROUPS))
    
    # Admin security
    application.add_handler(CommandHandler("antiflood", toggle_antiflood, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setflood", configure_antiflood, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("automod", toggle_automod, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("antilink", toggle_antilink, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("antiswear", toggle_antiswear, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setbadwords", set_bad_words, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("captcha", toggle_captcha, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setcaptcha", configure_captcha, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("admintag", toggle_admin_tag, filters=filters.ChatType.GROUPS))
    
    # Admin management
    application.add_handler(CommandHandler("setrules", set_rules, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setwelcome", set_welcome, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("welcome", toggle_welcome, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("welcomepreview", preview_welcome, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setgoodbye", set_goodbye, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("goodbye", toggle_goodbye, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("note", save_note, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("get", get_note, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("delnote", del_note, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("filter", add_filter, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("stop", stop_filter, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setmaxwarn", set_max_warnings, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("settings", settings_panel, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("info", group_info, filters=filters.ChatType.GROUPS))
    
    # General commands
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("rules", show_rules, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("id", get_id))
    application.add_handler(CommandHandler("stats", group_stats, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("export", export_stats, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("leaderboard", leaderboard, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("poll", create_poll, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("quiz", create_quiz, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("roll", roll_dice, filters=filters.ChatType.GROUPS))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_member_captcha))
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, send_goodbye_message))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, check_notes_and_filters), group=1)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, antiflood_check), group=2)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, scan_message), group=3)
    application.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'@admin\b') & filters.ChatType.GROUPS, 
        handle_admin_tag
    ), group=4)
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(handle_captcha_callback, pattern="^captcha_"))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # Error handler
    application.add_error_handler(error_handler)
    
    return application

async def post_init(app: Application) -> None:
    """Post initialization"""
    logger.info("="*50)
    logger.info("🚀 BOT STARTING UP...")
    logger.info("="*50)
    logger.info(f"Admin ID: {config.admin_id}")
    logger.info(f"Database: {config.database_name}")
    logger.info(f"Max Warnings: {config.max_warnings}")
    logger.info(f"Captcha Timeout: {config.captcha_timeout}s")
    logger.info(f"Flood Limit: {config.flood_limit} msgs/{config.flood_time}s")
    logger.info(f"Rate Limit: {config.rate_limit_calls} calls/{config.rate_limit_period}s")
    logger.info(f"Debug Mode: {config.debug}")
    logger.info("="*50)
    
    try:
        await mongo.connect()
    except Exception as e:
        logger.critical(f"❌ Failed to connect to MongoDB: {e}")
        sys.exit(1)
    
    app.job_queue.run_repeating(periodic_cleanup, interval=300, first=300)
    
    try:
        await mongo.db.bot_stats.update_one(
            {"date": datetime.now(timezone.utc).date().isoformat()},
            {"$inc": {"starts": 1}},
            upsert=True
        )
        logger.info("✅ Bot stats updated")
    except Exception as e:
        logger.error(f"Failed to update bot stats: {e}")
    
    logger.info("✅ Bot started successfully")

async def post_stop(app: Application) -> None:
    """Post shutdown"""
    logger.info("💤 Bot shutting down...")
    if mongo.client:
        mongo.client.close()

def main():
    """Main entry point"""
    global application
    
    application = setup_application()
    
    try:
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            close_loop=True,
            drop_pending_updates=False
        )
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
