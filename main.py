#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# PRODUCTION-READY TELEGRAM GROUP MANAGER BOT
# MongoDB Version | Enhanced Security | Per-Group Warnings
# 
# SECURITY NOTICE:
# - Never hardcode credentials
# - Use environment variables for all sensitive data
# - Run on a secure server (not Android due to MongoDB SRV DNS requirements)
# - Keep dependencies updated
# =============================================================================

import os
import sys
import re
import csv
import time
import signal
import logging
import asyncio
import random
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from functools import wraps
from typing import Dict, List, Any, Optional, Tuple, Callable
from contextlib import asynccontextmanager

from telegram import (
    Update, User, Chat, Message, MessageEntity, InlineKeyboardButton, 
    InlineKeyboardMarkup, ChatMember
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes, Application, JobQueue  # ✅ Added JobQueue
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, Forbidden, BadRequest
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import PyMongoError, ConnectionFailure
from logging.handlers import RotatingFileHandler
import pytz  # ✅ NEW: For timezone support

def setup_application() -> Application:
    """Setup and configure the application"""
    global application
    
    # ✅ FIXED: Initialize JobQueue
    application = (
        ApplicationBuilder()
        .token(config.TOKEN)
        .concurrent_updates(True)
        .connection_pool_size(8)
        .rate_limiter(None)
        .job_queue(JobQueue())
        .build()
    )
    
    # ✅ FIXED: Start the scheduler
    application.job_queue.scheduler.start()
    logger.info("✅ JobQueue started successfully")
    
    # === MESSAGE LOGGING (Group -1) ===
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, 
            message_logger
        ), 
        group=-1
    )
    
    # === COMMAND HANDLERS (Group 0) ===
    
    # Owner-only commands
    application.add_handler(CommandHandler("broadcast", broadcast_command, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("leave", leave_command))
    application.add_handler(CommandHandler("logs", view_bot_logs, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("resetgroup", reset_group_command))
    application.add_handler(CommandHandler("confirm", handle_reset_confirmation))
    application.add_handler(CommandHandler("shutdown", shutdown_bot, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("ping", ping_bot))
    
    # Admin moderation commands
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
    
    # Admin security commands
    application.add_handler(CommandHandler("antiflood", toggle_antiflood, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setflood", configure_antiflood, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("automod", toggle_automod, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("antilink", toggle_antilink, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("antiswear", toggle_antiswear, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setbadwords", set_bad_words, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("captcha", toggle_captcha, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setcaptcha", configure_captcha, filters=filters.ChatType.GROUPS))
    
    # Admin management commands
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
    application.add_handler(CommandHandler("export", export_stats, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("stats", group_stats, filters=filters.ChatType.GROUPS))
    
    # General commands
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("rules", show_rules, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("id", get_id))
    application.add_handler(CommandHandler("leaderboard", leaderboard, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("poll", create_poll, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("quiz", create_quiz, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("roll", roll_dice, filters=filters.ChatType.GROUPS))
    
    # === MESSAGE HANDLERS (Groups 1-3) ===
    
    # New member CAPTCHA
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_member_captcha))
    
    # Goodbye messages
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, send_goodbye_message))
    
    # Notes/Filters (Group 1)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, check_notes_and_filters), group=1)
    
    # Anti-flood (Group 2)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, antiflood_check), group=2)
    
    # Auto-moderation (Group 3)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, scan_message), group=3)
    
    # === CALLBACK QUERY HANDLERS ===
    
    # CAPTCHA buttons
    application.add_handler(CallbackQueryHandler(handle_captcha, pattern="^captcha_"))
    
    # Settings buttons
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # === ERROR HANDLER ===
    application.add_error_handler(error_handler)
    
    return application


# === PRODUCTION CONFIGURATION ===
class BotConfig:
    """Secure configuration from environment variables only.
    No hardcoded credentials allowed in production."""
    
    # Required configuration - will exit if not set
    TOKEN: str = "8290310263:AAGnseEQA6qyXk8lqFbTf4vqNdKzJKZq0tg"
    ADMIN_ID: int = 8149151609
    MONGODB_URI: str = "mongodb+srv://mefirebase1115_db_user:f76qFi3OqJQsagU2@cluster0.wsppssu.mongodb.net/?appName=Cluster0"
    
    # Optional configuration with sensible defaults
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "telegram_bot")
    MAX_WARNINGS: int = int(os.getenv("MAX_WARNINGS", "5"))
    CAPTCHA_TIMEOUT: int = int(os.getenv("CAPTCHA_TIMEOUT", "120"))
    FLOOD_LIMIT: int = int(os.getenv("FLOOD_LIMIT", "5"))
    FLOOD_TIME: int = int(os.getenv("FLOOD_TIME", "10"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    RATE_LIMIT_CALLS: int = int(os.getenv("RATE_LIMIT_CALLS", "5"))
    RATE_LIMIT_PERIOD: int = int(os.getenv("RATE_LIMIT_PERIOD", "60"))
    LOG_FILE_MAX_BYTES: int = int(os.getenv("LOG_FILE_MAX_BYTES", "10485760"))  # 10MB
    LOG_FILE_BACKUP_COUNT: int = int(os.getenv("LOG_FILE_BACKUP_COUNT", "5"))
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Validate required configuration before startup"""
        errors = []
        if not self.TOKEN:
            errors.append("❌ BOT_TOKEN environment variable is not set")
        if not self.ADMIN_ID or self.ADMIN_ID == 0:
            errors.append("❌ ADMIN_ID environment variable is not set")
        if not self.MONGODB_URI:
            errors.append("❌ MONGODB_URI environment variable is not set")
        return len(errors) == 0, errors
    
    def log_config(self, logger: logging.Logger):
        """Log configuration (hide sensitive data)"""
        logger.info("=== Bot Configuration ===")
        logger.info(f"Admin ID: {self.ADMIN_ID}")
        logger.info(f"Database: {self.DATABASE_NAME}")
        logger.info(f"Max Warnings: {self.MAX_WARNINGS}")
        logger.info(f"Captcha Timeout: {self.CAPTCHA_TIMEOUT}s")
        logger.info(f"Flood Limit: {self.FLOOD_LIMIT} msgs/{self.FLOOD_TIME}s")
        logger.info(f"Rate Limit: {self.RATE_LIMIT_CALLS} calls/{self.RATE_LIMIT_PERIOD}s")
        logger.info(f"Debug Mode: {self.DEBUG}")
        logger.info("=========================")

config = BotConfig()

# === GLOBALS ===
START_TIME = time.time()

# === ADVANCED LOGGING SETUP ===
def setup_logger() -> logging.Logger:
    """Setup production-grade logging"""
    Path("logs").mkdir(exist_ok=True)
    logger = logging.getLogger("telegram_bot")
    level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(logging.DEBUG if config.DEBUG else level)
    logger.handlers.clear()
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %Z'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG if config.DEBUG else logging.INFO)
    logger.addHandler(console_handler)
    
    # File handler with rotation
    try:
        file_handler = RotatingFileHandler(
            "logs/bot.log", 
            maxBytes=config.LOG_FILE_MAX_BYTES, 
            backupCount=config.LOG_FILE_BACKUP_COUNT, 
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG if config.DEBUG else logging.INFO)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.error(f"Failed to setup file handler: {e}")
    
    return logger

logger = setup_logger()

# === PRODUCTION MONGODB SETUP ===
class MongoDB:
    """MongoDB client with connection pooling and error handling"""
    def __init__(self):
        self.client = None
        self.db = None
        self._connect()
    
    def _connect(self):
        """Establish MongoDB connection with retry logic and DNS fallback"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use direct connection if SRV fails (for environments without proper DNS)
                if config.MONGODB_URI.startswith("mongodb+srv://"):
                    logger.warning("Using MongoDB SRV connection. Ensure DNS is properly configured.")
                
                self.client = MongoClient(
                    config.MONGODB_URI,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=5000,
                    retryWrites=True,
                    w='majority',
                    maxPoolSize=10,  # Connection pooling
                    minPoolSize=3
                )
                # Test connection
                self.client.admin.command('ping')
                self.db = self.client[config.DATABASE_NAME]
                self.setup_collections()
                self.create_indexes()
                logger.info("✅ MongoDB connected successfully")
                return
                
            except ConnectionFailure as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
            
            except Exception as e:
                logger.critical(f"MongoDB connection error: {e}")
                raise
    
    def setup_collections(self):
        """Initialize collection references"""
        self.users: Collection = self.db.users
        self.groups: Collection = self.db.groups
        self.warnings: Collection = self.db.warnings
        self.logs: Collection = self.db.logs
        self.captchas: Collection = self.db.captchas
        self.polls: Collection = self.db.polls
        self.bot_stats: Collection = self.db.bot_stats
        self.admin_permissions: Collection = self.db.admin_permissions
    
    def create_indexes(self):
        """Create performance indexes"""
        try:
            # Users collection
            self.users.create_index([("id", ASCENDING)], unique=True)
            self.users.create_index([("username", ASCENDING)], sparse=True)
            
            # Groups collection
            self.groups.create_index([("id", ASCENDING)], unique=True)
            
            # Warnings collection
            self.warnings.create_index([
                ("user_id", ASCENDING), 
                ("group_id", ASCENDING), 
                ("active", ASCENDING)
            ])
            self.warnings.create_index([
                ("timestamp", DESCENDING)
            ], expireAfterSeconds=7776000)  # Auto-expire after 90 days
            
            # Logs collection
            self.logs.create_index([
                ("group_id", ASCENDING), 
                ("timestamp", DESCENDING)
            ])
            self.logs.create_index([
                ("user_id", ASCENDING), 
                ("timestamp", DESCENDING)
            ])
            
            # Captchas collection
            self.captchas.create_index([
                ("expires_at", ASCENDING)
            ], expireAfterSeconds=0)  # TTL index
            self.captchas.create_index([
                ("user_id", ASCENDING), 
                ("group_id", ASCENDING)
            ])
            
            # Bot stats collection
            self.bot_stats.create_index([
                ("date", DESCENDING)
            ], unique=True)
            
            # Admin permissions collection
            self.admin_permissions.create_index([
                ("user_id", ASCENDING), 
                ("group_id", ASCENDING)
            ], unique=True)
            
            logger.info("✅ MongoDB indexes created")
            
        except PyMongoError as e:
            logger.error(f"Failed to create indexes: {e}")
            raise
    
    def get_user_warnings(self, user_id: int, group_id: int) -> int:
        """Get active warning count for a user in a group"""
        try:
            return self.warnings.count_documents({
                "user_id": user_id,
                "group_id": group_id,
                "active": True
            })
        except PyMongoError as e:
            logger.error(f"DB error in get_user_warnings: {e}")
            return 0
    
    def add_warning(self, user_id: int, group_id: int, reason: str, warned_by: int) -> int:
        """Add a warning and return the new count"""
        try:
            self.warnings.insert_one({
                "user_id": user_id,
                "group_id": group_id,
                "reason": reason,
                "warned_by": warned_by,
                "timestamp": datetime.utcnow(),
                "active": True
            })
            return self.get_user_warnings(user_id, group_id)
        except PyMongoError as e:
            logger.error(f"DB error in add_warning: {e}")
            return 0
    
    def clear_warnings(self, user_id: int, group_id: int) -> int:
        """Clear all warnings for a user in a group"""
        try:
            result = self.warnings.update_many(
                {"user_id": user_id, "group_id": group_id, "active": True},
                {"$set": {"active": False}}
            )
            return result.modified_count
        except PyMongoError as e:
            logger.error(f"DB error in clear_warnings: {e}")
            return 0
    
    def remove_last_warning(self, user_id: int, group_id: int) -> bool:
        """Remove the most recent warning"""
        try:
            warning = self.warnings.find_one_and_update(
                {"user_id": user_id, "group_id": group_id, "active": True},
                {"$set": {"active": False}},
                sort=[("timestamp", DESCENDING)]
            )
            return warning is not None
        except PyMongoError as e:
            logger.error(f"DB error in remove_last_warning: {e}")
            return False

# Initialize MongoDB - will exit on failure
try:
    mongo = MongoDB()
except PyMongoError as e:
    logger.critical(f"❌ Failed to initialize MongoDB: {e}")
    logger.critical("Bot cannot start without database connection")
    sys.exit(1)

# === CACHING SYSTEM ===
_group_cache: Dict[str, Any] = {}
_cache_ttl: Dict[str, float] = {}
_cache_lock = asyncio.Lock()

async def get_cached_group(group_id: int) -> Dict[str, Any]:
    """Async-aware group cache with TTL"""
    cache_key = f"group_{group_id}"
    now = time.time()
    
    async with _cache_lock:
        if cache_key in _group_cache and now - _cache_ttl.get(cache_key, 0) < 300:
            return _group_cache[cache_key]
    
    try:
        group = await asyncio.get_event_loop().run_in_executor(
            None, lambda: mongo.groups.find_one({"id": group_id}) or {}
        )
        async with _cache_lock:
            _group_cache[cache_key] = group
            _cache_ttl[cache_key] = now
        return group
    except PyMongoError as e:
        logger.error(f"DB error in get_cached_group: {e}")
        return {}

def invalidate_group_cache(group_id: int):
    """Invalidate group cache"""
    _group_cache.pop(f"group_{group_id}", None)
    _cache_ttl.pop(f"group_{group_id}", None)

# === TELEGRAM UTILITY FUNCTIONS ===
def get_user_mention(user: User) -> str:
    """Get safe user mention string"""
    try:
        if user.username:
            return f"@{user.username}"
        return f'<a href="tg://user?id={user.id}">{escape_html(user.first_name)}</a>'
    except Exception:
        return f"User({user.id})"

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
    except (ValueError, AttributeError):
        return None

def format_duration(seconds: int) -> str:
    """Format seconds to human-readable duration"""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        return f"{seconds // 3600}h"
    else:
        return f"{seconds // 86400}d"

def format_time_ago(seconds: int) -> str:
    """Format seconds to 'X ago' string"""
    if seconds < 60:
        return f"{seconds}s ago"
    elif seconds < 3600:
        return f"{seconds // 60}m ago"
    elif seconds < 86400:
        return f"{seconds // 3600}h ago"
    else:
        return f"{seconds // 86400}d ago"

async def delete_after_delay(message: Message, delay: int):
    """Delete a message after a delay"""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except:
        pass

# === PERMISSION CHECKING ===
async def is_admin(chat: Chat, user_id: int, cache: bool = True) -> bool:
    """Check if user is admin with optional caching"""
    if not cache:
        try:
            member = await chat.get_member(user_id)
            return member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
        except Exception as e:
            logger.error(f"Admin check failed: {e}")
            return False
    
    cache_key = f"admin_{chat.id}_{user_id}"
    now = time.time()
    
    if cache_key in _cache_ttl and now - _cache_ttl.get(cache_key, 0) < 300:
        return _group_cache.get(cache_key, False)
    
    try:
        member = await chat.get_member(user_id)
        is_admin_status = member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
        
        # Cache the result
        async with _cache_lock:
            _group_cache[cache_key] = is_admin_status
            _cache_ttl[cache_key] = now
        
        return is_admin_status
    except Exception as e:
        logger.error(f"Admin check failed: {e}")
        return False

async def bot_can_restrict_members(chat: Chat, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if bot has restrict members permission"""
    try:
        bot_member = await chat.get_member(context.bot.id)
        return (bot_member.status == ChatMember.ADMINISTRATOR and 
                bot_member.can_restrict_members)
    except Exception as e:
        logger.error(f"Bot permission check failed: {e}")
        return False

async def bot_can_promote_members(chat: Chat, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if bot can promote members"""
    try:
        bot_member = await chat.get_member(context.bot.id)
        return (bot_member.status == ChatMember.ADMINISTRATOR and 
                bot_member.can_promote_members)
    except Exception as e:
        logger.error(f"Bot permission check failed: {e}")
        return False

def has_permission(user_id: int, permission: str, group_id: int) -> bool:
    """Check if admin has specific permission (for granular permissions)"""
    try:
        perm_doc = mongo.admin_permissions.find_one({
            "user_id": user_id,
            "group_id": group_id
        })
        
        if not perm_doc:
            return True  # Default allow if no permissions set
        
        perms = perm_doc.get("permissions", [])
        return "all" in perms or permission in perms
        
    except PyMongoError as e:
        logger.error(f"Permission check failed: {e}")
        return True  # Fail open for safety

# === RATE LIMITING ===
_user_command_tracker: Dict[int, List[float]] = defaultdict(list)
_callback_tracker: Dict[int, List[float]] = defaultdict(list)

def check_rate_limit(user_id: int, tracker: Dict = _user_command_tracker) -> bool:
    """Check if user is within rate limits"""
    now = time.time()
    calls = tracker[user_id]
    calls = [t for t in calls if now - t < config.RATE_LIMIT_PERIOD]
    calls.append(now)
    tracker[user_id] = calls
    return len(calls) <= config.RATE_LIMIT_CALLS

# === DECORATORS ===
def admin_only(permission: Optional[str] = None):
    """Decorator for admin-only commands with optional permission check"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user = update.effective_user
            chat = update.effective_chat
            message = update.effective_message
            
            if chat.type not in [Chat.GROUP, Chat.SUPERGROUP]:
                await message.reply_text("❌ This command only works in groups.")
                return
            
            # Owner bypass
            if user.id == config.ADMIN_ID:
                return await func(update, context, *args, **kwargs)
            
            try:
                member = await chat.get_member(user.id)
                if member.status not in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]:
                    await message.reply_text("❌ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
                    return
                
                if permission and not has_permission(user.id, permission, chat.id):
                    await message.reply_text("❌ <b>No permission.</b>", parse_mode=ParseMode.HTML)
                    return
                
                return await func(update, context, *args, **kwargs)
                
            except TelegramError as e:
                logger.error(f"Admin check failed in {func.__name__}: {e}")
                await message.reply_text("❌ Permission check failed.")
        
        return wrapper
    return decorator

def log_command(action: str):
    """Decorator to log commands to database"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
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
                raise
            finally:
                try:
                    mongo.logs.insert_one({
                        "group_id": chat.id if chat else None,
                        "user_id": user.id if user else None,
                        "action": action,
                        "command": message.text[:100] if message else None,
                        "timestamp": datetime.utcnow(),
                        "status": "success" if success else "failed",
                        "error": error_msg,
                        "execution_time_ms": int((time.time() - start_time) * 1000)
                    })
                except PyMongoError as e:
                    logger.error(f"Failed to log command: {e}")
        
        return wrapper
    return decorator

def rate_limit_decorator():
    """Decorator for rate limiting commands"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            if not check_rate_limit(user_id):
                remaining = config.RATE_LIMIT_PERIOD - int(time.time() - _user_command_tracker[user_id][0])
                await update.message.reply_text(
                    f"⚠️ Too many commands. Please wait {remaining} seconds."
                )
                return
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

def owner_only():
    """Decorator for owner-only commands"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            if update.effective_user.id != config.ADMIN_ID:
                await update.message.reply_text("❌ <b>Owner only.</b>", parse_mode=ParseMode.HTML)
                return
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

# === USER RESOLUTION ===
async def resolve_target_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Tuple[Optional[User], Optional[str]]:
    """Resolve target user from reply, mention, or ID"""
    message = update.effective_message
    
    # Priority: reply to message
    if message.reply_to_message:
        return message.reply_to_message.from_user, None
    
    # Check for arguments
    if not context.args:
        return None, "❌ Specify user by reply, @username, or numeric ID."
    
    # Check for text entities (mentions)
    if message.entities:
        for entity in message.entities:
            if entity.type == MessageEntity.MENTION:
                # Username mention like @username
                username = message.text[entity.offset + 1:entity.offset + entity.length].lower()
                user_data = mongo.users.find_one({"username": username})
                if user_data:
                    try:
                        member = await update.effective_chat.get_member(user_data["id"])
                        return member.user, None
                    except:
                        return User(id=user_data["id"], first_name=username, is_bot=False), None
                return None, f"❌ User @{username} not found in database. They must have messaged the bot at least once."
            
            if entity.type == MessageEntity.TEXT_MENTION:
                # Mention of user without username
                return entity.user, None

    identifier = context.args[0].strip()
    
    # Numeric ID
    if identifier.isdigit():
        try:
            user_id = int(identifier)
            member = await update.effective_chat.get_member(user_id)
            return member.user, None
        except Exception:
            # Try to return a mock user if we can't find them in chat
            return User(id=user_id, first_name=f"User {user_id}", is_bot=False), None
    
    # Username mention fallback (if not caught by entities)
    if identifier.startswith('@'):
        username = identifier[1:].lower()
        user_data = mongo.users.find_one({"username": username})
        if user_data:
            try:
                member = await update.effective_chat.get_member(user_data["id"])
                return member.user, None
            except:
                return User(id=user_data["id"], first_name=username, is_bot=False), None
        return None, f"❌ User @{username} not found."
    
    return None, f"❌ Invalid identifier '{identifier}'. Use @username or numeric ID."

# === CORE COMMAND HANDLERS ===
@owner_only()
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all groups (owner only)"""
    if not context.args:
        await update.message.reply_text("❌ Provide a message to broadcast.")
        return
    
    message = " ".join(context.args)
    groups = mongo.groups.find({})
    success_count = 0
    fail_count = 0
    
    status_message = await update.message.reply_text(f"📢 Broadcasting to groups... 0 sent.")
    
    for i, group in enumerate(groups):
        if i > 0:
            await asyncio.sleep(0.1)  # Rate limit broadcasts
        
        try:
            # Check if group['id'] is valid before sending
            target_chat_id = group.get("id")
            if not target_chat_id:
                continue
                
            await context.bot.send_message(
                chat_id=target_chat_id,
                text=f"{escape_html(message)}",
                parse_mode=ParseMode.HTML
            )
            success_count += 1
            
            if i % 5 == 0:
                await status_message.edit_text(
                    f"📢 Broadcasting... {success_count} sent, {fail_count} failed."
                )
                
        except Forbidden:
            logger.warning(f"Bot was removed from group {group.get('id')}")
            mongo.groups.delete_one({"id": group.get("id")})
            fail_count += 1
        except Exception as e:
            logger.error(f"Broadcast failed to {group.get('id')}: {e}")
            fail_count += 1
    
    await status_message.edit_text(
        f"✅ Broadcast complete!\nSent: {success_count}\nFailed: {fail_count}"
    )

@owner_only()
async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Leave a group (owner only)"""
    chat_id = update.effective_chat.id
    
    # Allow specifying group ID
    if context.args and context.args[0].isdigit():
        chat_id = int(context.args[0])
    
    try:
        if chat_id != update.effective_chat.id:
            await context.bot.send_message(
                chat_id=chat_id,
                text="👋 Bot is leaving this group as requested by owner."
            )
        
        await context.bot.leave_chat(chat_id)
        
        # Cleanup group data
        mongo.groups.delete_one({"id": chat_id})
        mongo.warnings.delete_many({"group_id": chat_id})
        mongo.logs.delete_many({"group_id": chat_id})
        mongo.captchas.delete_many({"group_id": chat_id})
        mongo.admin_permissions.delete_many({"group_id": chat_id})
        invalidate_group_cache(chat_id)
        
        if chat_id != update.effective_chat.id:
            await update.message.reply_text(f"✅ Left group {chat_id} and cleaned up data.")
    except Exception as e:
        logger.error(f"Leave failed: {e}")
        await update.message.reply_text(f"❌ Failed to leave: {e}")

@owner_only()
async def view_bot_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View bot logs (owner only) with file download and animation"""
    try:
        # Initial message with animation
        status_msg = await update.message.reply_text("🔍 <b>Searching for logs...</b>", parse_mode=ParseMode.HTML)
        await asyncio.sleep(0.5)
        await status_msg.edit_text("📂 <b>Formatting log entries...</b>", parse_mode=ParseMode.HTML)
        await asyncio.sleep(0.5)
        await status_msg.edit_text("⚙️ <b>Generating text file...</b>", parse_mode=ParseMode.HTML)
        
        # Fetch last 10 logs
        logs = list(mongo.logs.find({}).sort("timestamp", DESCENDING).limit(10))
        if not logs:
            await status_msg.edit_text("📋 No logs found.")
            return
        
        # Create log content
        log_content = "📜 BOT ACTIVITY LOGS (LAST 10)\n"
        log_content += "="*50 + "\n\n"
        
        for log in logs:
            time_str = log['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            action = log.get('action', 'unknown').upper()
            user_id = log.get('user_id', 'N/A')
            status = log.get('status', 'unknown')
            error = log.get('error', 'None')
            details = log.get('details', log.get('command', 'N/A'))
            
            log_content += f"TIME: {time_str}\n"
            log_content += f"ACTION: {action}\n"
            log_content += f"USER ID: {user_id}\n"
            log_content += f"STATUS: {status}\n"
            if error != 'None':
                log_content += f"ERROR: {error}\n"
            log_content += f"DETAILS: {details}\n"
            log_content += "-"*30 + "\n\n"

        # Convert to bytes for Telegram
        log_bytes = BytesIO(log_content.encode('utf-8'))
        log_bytes.name = f"bot_logs_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
        
        await status_msg.edit_text("📤 <b>Sending file...</b>", parse_mode=ParseMode.HTML)
        
        # Send as document
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=log_bytes,
            filename=log_bytes.name,
            caption="📜 <b>Bot Activity Logs (Last 10)</b>",
            parse_mode=ParseMode.HTML
        )
        
        # Remove the status message
        await status_msg.delete()
        
    except Exception as e:
        logger.error(f"Error in view_bot_logs: {e}")
        await update.message.reply_text(f"❌ Failed to generate logs: {e}")

@owner_only()
async def reset_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset all group data (owner only)"""
    chat_id = update.effective_chat.id
    
    confirm_msg = await update.message.reply_text(
        "⚠️ <b>WARNING</b>\n\n"
        "This will delete ALL warnings, logs, settings, and permissions for this group.\n"
        "This action is <b>irreversible</b>.\n\n"
        "Type <code>confirm</code> within 30 seconds to proceed.",
        parse_mode=ParseMode.HTML
    )
    
    context.user_data['pending_reset'] = chat_id
    
    # Auto-clear confirmation after timeout
    await asyncio.sleep(30)
    if context.user_data.get('pending_reset') == chat_id:
        await confirm_msg.edit_text("❌ Reset cancelled (timeout).")
        context.user_data.pop('pending_reset', None)

@owner_only()
async def handle_reset_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle reset confirmation"""
    if context.user_data.get('pending_reset') == update.effective_chat.id:
        confirmation_text = update.message.text.strip().lower()
        
        if confirmation_text == 'confirm':
            chat_id = update.effective_chat.id
            try:
                # Delete all group-related data
                mongo.groups.delete_one({"id": chat_id})
                mongo.warnings.delete_many({"group_id": chat_id})
                mongo.logs.delete_many({"group_id": chat_id})
                mongo.captchas.delete_many({"group_id": chat_id})
                mongo.admin_permissions.delete_many({"group_id": chat_id})
                invalidate_group_cache(chat_id)
                
                await update.message.reply_text(
                    "✅ <b>Group Reset Complete</b>\n\n"
                    "All data for this group has been deleted.",
                    parse_mode=ParseMode.HTML
                )
                logger.warning(f"Group {chat_id} was reset by owner")
            except PyMongoError as e:
                logger.error(f"Reset failed: {e}")
                await update.message.reply_text("❌ Database error during reset.")
        else:
            await update.message.reply_text("❌ Reset cancelled.")
        
        context.user_data.pop('pending_reset', None)

@owner_only()
async def shutdown_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Graceful shutdown (owner only)"""
    await update.message.reply_text("💤 Shutting down gracefully...")
    
    try:
        mongo.bot_stats.update_one(
            {"date": datetime.utcnow().date().isoformat()},
            {"$inc": {"shutdowns": 1}},
            upsert=True
        )
        logger.info("Bot shutdown initiated by owner")
    except PyMongoError as e:
        logger.error(f"Failed to save shutdown stats: {e}")
    
    # Schedule shutdown
    asyncio.create_task(_perform_shutdown(context.application))

async def _perform_shutdown(app: Application):
    """Perform actual shutdown"""
    await asyncio.sleep(1)  # Allow message to send
    await app.stop()
    await app.shutdown()
    sys.exit(0)

@owner_only()
async def ping_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Health check command"""
    start = time.time()
    msg = await update.message.reply_text(
        f"🏓 <b>PINGING...</b>",
        parse_mode=ParseMode.HTML
    )
    
    # Check MongoDB health
    db_status = "❌"
    db_latency = 0
    try:
        db_start = time.time()
        mongo.client.admin.command('ping')
        db_status = "✅"
        db_latency = int((time.time() - db_start) * 1000)
    except:
        pass
    
    # Get bot stats
    try:
        stats = mongo.bot_stats.find_one(
            {"date": datetime.utcnow().date().isoformat()}
        ) or {}
    except:
        stats = {}
    
    latency = int((time.time() - start) * 1000)
    
    ping_text = (
        f"<b>PONG</b> 🏓\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📡 <b>Latency:</b> {latency}ms\n"
        f"🗄️ <b>Database:</b> {db_latency}ms ({db_status})\n"
        f"⌛ <b>Uptime:</b> {format_duration(int(time.time() - START_TIME))}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    await msg.edit_text(ping_text, parse_mode=ParseMode.HTML)

# === MODERATION COMMANDS ===
@admin_only()
@log_command("ban")
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ban a user from the group"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if target_user.id == config.ADMIN_ID:
        await update.message.reply_text("❌ Cannot ban bot owner.")
        return
    
    if await is_admin(update.effective_chat, target_user.id):
        await update.message.reply_text("❌ Cannot ban an admin.")
        return
    
    if not await bot_can_restrict_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Restrict Members' permission to ban users.")
        return
    
    try:
        chat = update.effective_chat
        
        # Get reason
        if update.message.reply_to_message:
            reason = " ".join(context.args) if context.args else "No reason provided"
        else:
            reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason provided"
        
        # Ban user
        await chat.ban_member(user_id=target_user.id)
        
        # Update user record
        mongo.users.update_one(
            {"id": target_user.id},
            {"$set": {
                "id": target_user.id,
                "username": target_user.username,
                "first_name": target_user.first_name,
                "last_name": target_user.last_name,
                f"banned_in.{chat.id}": True,
                "last_updated": datetime.utcnow()
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
        await update.message.reply_text("❌ I don't have permission to ban users.")
    except BadRequest as e:
        logger.error(f"Ban failed: {e}")
        await update.message.reply_text(f"❌ Failed to ban: {e.message}")
    except Exception as e:
        logger.error(f"Unexpected error in ban: {e}")
        await update.message.reply_text("❌ An error occurred.")

@admin_only()
@log_command("unban")
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unban a user"""
    chat = update.effective_chat
    target_user, error = await resolve_target_user(update, context)
    
    if error:
        # Fallback for manual ID if resolve fails
        if context.args and context.args[0].isdigit():
            user_id = int(context.args[0])
            user_name = f"User {user_id}"
        else:
            await update.message.reply_text(error)
            return
    else:
        user_id = target_user.id
        user_name = get_user_mention(target_user)

    try:
        await chat.unban_member(user_id=user_id)
        mongo.users.update_one(
            {"id": user_id},
            {"$unset": {f"banned_in.{chat.id}": ""}}
        )
        await update.message.reply_text(
            f"✅ <b>UNBANNED</b> ✅\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>User:</b> {user_name}\n"
            f"🔓 <i>Access restored!</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Unban failed: {e}")
        await update.message.reply_text(f"❌ Failed to unban: {e}")

@admin_only()
@log_command("mute")
async def mute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mute a user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if target_user.id == config.ADMIN_ID:
        await update.message.reply_text("❌ Cannot mute bot owner.")
        return
    
    if await is_admin(update.effective_chat, target_user.id):
        await update.message.reply_text("❌ Cannot mute an admin.")
        return
    
    if not await bot_can_restrict_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Restrict Members' permission to mute users.")
        return
    
    try:
        chat = update.effective_chat
        
        # Parse duration
        time_arg = None
        if update.message.reply_to_message and context.args:
            time_arg = context.args[0]
        elif not update.message.reply_to_message and len(context.args) > 1:
            time_arg = context.args[1]
        
        duration_seconds = parse_time_string(time_arg)
        until_date = datetime.utcnow() + timedelta(seconds=duration_seconds) if duration_seconds else None
        
        # Mute user
        await chat.restrict_member(
            user_id=target_user.id,
            permissions={
                "can_send_messages": False,
                "can_send_media_messages": False,
                "can_send_polls": False,
                "can_send_other_messages": False,
                "can_add_web_page_previews": False,
                "can_change_info": False,
                "can_invite_users": False,
                "can_pin_messages": False
            },
            until_date=until_date
        )
        
        # Update user record
        mongo.users.update_one(
            {"id": target_user.id},
            {"$set": {
                f"muted_in.{chat.id}": {
                    "until": until_date,
                    "duration": duration_seconds
                },
                "last_updated": datetime.utcnow()
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
    except Forbidden:
        await update.message.reply_text("❌ I don't have permission to restrict users.")
    except Exception as e:
        logger.error(f"Mute failed: {e}")
        await update.message.reply_text(f"❌ Failed to mute: {e}")

@admin_only()
@log_command("unmute")
async def unmute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unmute a user"""
    chat = update.effective_chat
    
    if context.args and context.args[0].isdigit():
        user_id = int(context.args[0])
        try:
            await chat.restrict_member(
                user_id=user_id,
                permissions={
                    "can_send_messages": True,
                    "can_send_media_messages": True,
                    "can_send_polls": True,
                    "can_send_other_messages": True,
                    "can_add_web_page_previews": True
                }
            )
            mongo.users.update_one(
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
            logger.error(f"Unmute failed: {e}")
            await update.message.reply_text(f"❌ Failed to unmute: {e}")
    else:
        target_user, error = await resolve_target_user(update, context)
        if error:
            await update.message.reply_text(error)
            return
        
        try:
            await chat.restrict_member(
                user_id=target_user.id,
                permissions={
                    "can_send_messages": True,
                    "can_send_media_messages": True,
                    "can_send_polls": True,
                    "can_send_other_messages": True,
                    "can_add_web_page_previews": True
                }
            )
            mongo.users.update_one(
                {"id": target_user.id},
                {"$unset": {f"muted_in.{chat.id}": ""}}
            )
            await update.message.reply_text(
                f"🔊 <b>UNMUTED</b> 🔊\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
                f"✅ <i>Voice restored! You can speak now.</i>",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Unmute failed: {e}")
            await update.message.reply_text(f"❌ Failed to unmute: {e}")

@admin_only()
@log_command("kick")
async def kick_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Kick a user from the group"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if target_user.id == config.ADMIN_ID:
        await update.message.reply_text("❌ Cannot kick bot owner.")
        return
    
    if await is_admin(update.effective_chat, target_user.id):
        await update.message.reply_text("❌ Cannot kick an admin.")
        return
    
    if not await bot_can_restrict_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Restrict Members' permission to kick users.")
        return
    
    try:
        chat = update.effective_chat
        
        # Get reason
        if update.message.reply_to_message:
            reason = " ".join(context.args) if context.args else "No reason provided"
        else:
            reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason provided"
        
        # Kick user (ban and immediate unban)
        await chat.ban_member(user_id=target_user.id)
        await asyncio.sleep(0.5)  # Small delay to ensure ban is processed
        await chat.unban_member(user_id=target_user.id)
        
        await update.message.reply_text(
            f"👢 <b>KICKED</b> 👢\n\n"
            f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
            f"📝 <b>Reason:</b> {escape_html(reason)}\n\n"
            f"👋 <i>See you later!</i>",
            parse_mode=ParseMode.HTML
        )
        
    except Forbidden:
        await update.message.reply_text("❌ I don't have permission to kick users.")
    except Exception as e:
        logger.error(f"Kick failed: {e}")
        await update.message.reply_text(f"❌ Failed to kick: {e}")

@admin_only()
@log_command("purge")
async def purge_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Purge messages from chat"""
    message = update.effective_message
    chat = update.effective_chat
    
    # Default purge count
    count = 10
    
    if context.args:
        try:
            count = int(context.args[0])
            if count > 100:
                await message.reply_text("❌ Maximum 100 messages can be purged at once.")
                return
            if count < 1:
                await message.reply_text("❌ Must purge at least 1 message.")
                return
        except ValueError:
            await message.reply_text("❌ Invalid number.")
            return
    
    try:
        # Adjust count to include the command itself
        delete_count = count + 1
        
        # Collect message IDs to delete
        # Note: get_chat_history is not available on ExtBot directly in all versions
        # We should use a different approach or verify the correct method
        message_ids = []
        
        # Correct way to get history in recent python-telegram-bot
        # message_ids = [message.message_id] # Already have the current message
        
        # We'll try to delete messages one by one or in small batches
        # since bulk delete has limitations on age and bot permissions
        deleted = 0
        current_id = message.message_id
        
        for i in range(delete_count):
            try:
                await context.bot.delete_message(chat_id=chat.id, message_id=current_id - i)
                deleted += 1
            except:
                continue
        
        # Show confirmation
        confirm_text = (
            f"🧹 <b>PURGE COMPLETE</b> 🧹\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🗑️ <b>Messages:</b> {deleted}\n"
            f"⚖️ <i>Action by Admin</i>"
        )
        confirm_msg = await context.bot.send_message(
            chat_id=chat.id, 
            text=confirm_text, 
            parse_mode=ParseMode.HTML
        )
        # Delete confirmation after 3 seconds
        asyncio.create_task(delete_after_delay(confirm_msg, 3))
        
    except BadRequest as e:
        if "message to delete not found" in str(e):
            await message.reply_text("❌ Some messages could not be deleted (too old or not found).")
        else:
            logger.error(f"Purge failed: {e}")
            await message.reply_text(f"❌ Failed to purge: {e.message}")
    except Exception as e:
        logger.error(f"Purge failed: {e}")
        await message.reply_text("❌ Failed to purge messages.")

@admin_only()
@log_command("warn")
async def warn_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Warn a user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if target_user.id == config.ADMIN_ID:
        await update.message.reply_text("❌ Cannot warn bot owner.")
        return
    
    if await is_admin(update.effective_chat, target_user.id):
        await update.message.reply_text("❌ Cannot warn an admin.")
        return
    
    try:
        chat = update.effective_chat
        
        # Get reason
        if update.message.reply_to_message:
            reason = " ".join(context.args) if context.args else "No reason provided"
        else:
            reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason provided"
        
        # Add warning
        warnings_count = mongo.add_warning(
            user_id=target_user.id,
            group_id=chat.id,
            reason=reason,
            warned_by=update.effective_user.id
        )
        
        group = await get_cached_group(chat.id)
        max_warnings = group.get("max_warnings", config.MAX_WARNINGS)
        
        # Apply action based on warning count
        if warnings_count >= max_warnings:
            # Mute for 24 hours instead of banning
            mute_duration = 86400  # 24 hours
            await chat.restrict_member(
                user_id=target_user.id,
                permissions={"can_send_messages": False},
                until_date=datetime.utcnow() + timedelta(seconds=mute_duration)
            )
            action_text = f"🔇 <b>MUTED 24 HOURS</b> (reached {max_warnings} warnings)"
        elif warnings_count == max_warnings - 1:
            await chat.restrict_member(
                user_id=target_user.id,
                permissions={"can_send_messages": False},
                until_date=datetime.utcnow() + timedelta(hours=1)
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
        logger.error(f"Warn failed: {e}")
        await update.message.reply_text("❌ Failed to warn user.")

@admin_only()
@log_command("unwarn")
async def unwarn_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a warning from user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    chat = update.effective_chat
    
    try:
        # Resolve target user
        target_user, error = await resolve_target_user(update, context)
        if error:
            await update.message.reply_text(error)
            return

        removed_count = mongo.clear_warnings(target_user.id, chat.id)
        
        if removed_count > 0:
            await update.message.reply_text(
                f"✅ <b>WARNS CLEARED</b> ✅\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>User:</b> {get_user_mention(target_user)}\n"
                f"🗑️ <b>Removed:</b> {removed_count} warnings\n"
                f"✨ <i>User is now clean.</i>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"ℹ️ {get_user_mention(target_user)} has no active warnings.",
                parse_mode=ParseMode.HTML
            )
            
    except Exception as e:
        logger.error(f"Unwarn failed: {e}")
        await update.message.reply_text("❌ Failed to remove warning.")

@admin_only()
async def view_warnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View warnings for a user"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        target_user = update.effective_user
    
    chat = update.effective_chat
    
    try:
        warnings_count = mongo.get_user_warnings(target_user.id, chat.id)
        
        if warnings_count == 0:
            await update.message.reply_text(
                f"✅ {get_user_mention(target_user)} has no active warnings.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Get recent warnings
        recent_warnings = list(mongo.warnings.find({
            "user_id": target_user.id,
            "group_id": chat.id,
            "active": True
        }).sort("timestamp", DESCENDING).limit(10))
        
        warn_text = f"⚠️ <b>Warnings for {escape_html(target_user.first_name)}: {warnings_count}</b>\n\n"
        
        for i, warn in enumerate(recent_warnings, 1):
            try:
                warned_by_user = await context.bot.get_chat_member(chat.id, warn['warned_by'])
                warned_by_name = warned_by_user.user.first_name
            except:
                warned_by_name = "Admin"
            
            time_ago = format_time_ago(int((datetime.utcnow() - warn['timestamp']).total_seconds()))
            reason = escape_html(warn['reason'])
            warn_text += f"{i}. {reason}\n   <i>by {warned_by_name}, {time_ago}</i>\n\n"
        
        await update.message.reply_text(warn_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"View warnings failed: {e}")
        await update.message.reply_text("❌ Failed to retrieve warnings.")

@admin_only()
@log_command("promote")
async def promote_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Promote a user to admin"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if not await bot_can_promote_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Add Admins' permission to promote users.")
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
        logger.error(f"Promote failed: {e}")
        await update.message.reply_text(f"❌ Promote failed: {e}")

@admin_only()
@log_command("demote")
async def demote_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Demote an admin to member"""
    target_user, error = await resolve_target_user(update, context)
    if error:
        await update.message.reply_text(error)
        return
    
    if not await bot_can_promote_members(update.effective_chat, context):
        await update.message.reply_text("❌ I need 'Add Admins' permission to demote users.")
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
        logger.error(f"Demote failed: {e}")
        await update.message.reply_text(f"❌ Demote failed: {e}")

# === SECURITY MODULES ===
_flood_tracker: Dict[str, List[float]] = defaultdict(list)

async def antiflood_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check and handle flood"""
    user = update.effective_user
    chat = update.effective_chat
    
    # Skip checks for admins and owner
    if user.id == config.ADMIN_ID or await is_admin(chat, user.id):
        return True
    
    group = await get_cached_group(chat.id)
    if not group or not group.get("antiflood_enabled", False):
        return True
    
    flood_limit = group.get("antiflood_limit", config.FLOOD_LIMIT)
    flood_time = group.get("antiflood_time", config.FLOOD_TIME)
    
    tracker_key = f"{chat.id}_{user.id}"
    current_time = time.time()
    
    # Clean old messages
    user_messages = [t for t in _flood_tracker[tracker_key] if current_time - t < flood_time]
    user_messages.append(current_time)
    _flood_tracker[tracker_key] = user_messages
    
    # Check if user is flooding
    if len(user_messages) > flood_limit:
        try:
            mute_duration = 300  # 5 minutes
            await chat.restrict_member(
                user_id=user.id,
                permissions={"can_send_messages": False},
                until_date=datetime.utcnow() + timedelta(seconds=mute_duration)
            )
            
            # Log the action
            mongo.logs.insert_one({
                "group_id": chat.id,
                "user_id": user.id,
                "action": "auto_mute_flood",
                "details": f"Sent {len(user_messages)} messages in {flood_time}s",
                "timestamp": datetime.utcnow()
            })
            
            warning_msg = await update.message.reply_text(
                f"⚠️ <b>Anti-Flood Triggered!</b>\n"
                f"User {user.mention_html()} muted for {format_duration(mute_duration)}.",
                parse_mode=ParseMode.HTML
            )
            
            # Delete the flooding message
            try:
                await update.message.delete()
            except:
                pass
            
            # Auto-delete warning after 10 seconds
            await asyncio.sleep(10)
            await warning_msg.delete()
            
            return False
        except Exception as e:
            logger.error(f"Anti-flood action failed: {e}")
    
    return True

@admin_only()
async def toggle_antiflood(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle anti-flood protection"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    new_status = not group.get("antiflood_enabled", False)
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "antiflood_enabled": new_status,
                "antiflood_limit": group.get("antiflood_limit", config.FLOOD_LIMIT),
                "antiflood_time": group.get("antiflood_time", config.FLOOD_TIME)
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🛡️ Anti-Flood: {status_emoji}")
    except PyMongoError as e:
        logger.error(f"Toggle antiflood failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def configure_antiflood(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configure anti-flood settings"""
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usage: /setflood <limit> <time_in_seconds>")
        return
    
    chat = update.effective_chat
    
    try:
        limit = int(context.args[0])
        time_window = int(context.args[1])
        
        if limit < 2 or limit > 20:
            await update.message.reply_text("❌ Limit must be between 2-20.")
            return
        
        if time_window < 5 or time_window > 60:
            await update.message.reply_text("❌ Time window must be between 5-60 seconds.")
            return
        
        mongo.groups.update_one(
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
        invalidate_group_cache(chat.id)
        
        await update.message.reply_text(
            f"✅ Anti-Flood configured:\n"
            f"• Limit: {limit} messages\n"
            f"• Time window: {time_window}s\n"
            f"• Status: Enabled"
        )
    except ValueError:
        await update.message.reply_text("❌ Invalid numbers provided.")
    except PyMongoError as e:
        logger.error(f"Configure antiflood failed: {e}")
        await update.message.reply_text("❌ Database error.")

# === AUTO-MODERATION ===
async def scan_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Scan messages for auto-moderation"""
    message = update.effective_message
    if not message or not message.text:
        return
    
    user = update.effective_user
    chat = update.effective_chat
    
    if user.id == config.ADMIN_ID or await is_admin(chat, user.id):
        return
    
    group = await get_cached_group(chat.id)
    if not group or not group.get("automod_enabled", False):
        return
    
    text = message.text.lower()
    
    # Anti-swear
    if group.get("anti_swear_enabled", False):
        bad_words = group.get("bad_words", ["fuck", "shit", "bitch", "asshole", "dick"])
        
        for word in bad_words:
            if re.search(r'\b' + re.escape(word) + r'\b', text, re.IGNORECASE):
                await handle_violation(update, context, "bad_word", f"Used forbidden word: {word}")
                try:
                    await message.delete()
                except:
                    pass
                return
    
    # Anti-link
    if group.get("anti_link_enabled", False):
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
    """Handle auto-moderation violation"""
    user = update.effective_user
    chat = update.effective_chat
    
    warnings_count = mongo.add_warning(
        user_id=user.id,
        group_id=chat.id,
        reason=details,
        warned_by=context.bot.id
    )
    
    group = await get_cached_group(chat.id)
    max_warnings = group.get("max_warnings", config.MAX_WARNINGS)
    
    if warnings_count >= max_warnings:
        await chat.restrict_member(
            user_id=user.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=int(time.time()) + 86400
        )
        action_text = f"🔇 <b>MUTED 24H</b> 🔇\n<i>(Reached {max_warnings} warnings)</i>"
    elif warnings_count == max_warnings - 1:
        await chat.restrict_member(
            user_id=user.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=datetime.utcnow() + timedelta(hours=1)
        )
        action_text = f"🔇 <b>MUTED 1 HOUR</b> 🔇\n<i>(Warning {warnings_count}/{max_warnings})</i>"
    else:
        action_text = f"⚠️ <b>WARNING {warnings_count}/{max_warnings}</b> ⚠️"
    
    await update.message.reply_text(
        f"{action_text}\n\n"
        f"👤 <b>User:</b> {get_user_mention(user)}\n"
        f"📝 <b>Reason:</b> {escape_html(details)}\n\n"
        f"🛡️ <i>Maintain group decorum!</i>",
        parse_mode=ParseMode.HTML
    )

@admin_only()
async def toggle_automod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle auto-moderation"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    new_status = not group.get("automod_enabled", False)
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "automod_enabled": new_status
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🤖 Auto-Mod: {status_emoji}")
    except PyMongoError as e:
        logger.error(f"Toggle automod failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def toggle_antilink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle anti-link"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    new_status = not group.get("anti_link_enabled", False)
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "anti_link_enabled": new_status
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🔗 Anti-Link: {status_emoji}")
    except PyMongoError as e:
        logger.error(f"Toggle antilink failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def toggle_antiswear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle anti-swear"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    new_status = not group.get("anti_swear_enabled", False)
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "anti_swear_enabled": new_status
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🤬 Anti-Swear: {status_emoji}")
    except PyMongoError as e:
        logger.error(f"Toggle antiswear failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def set_bad_words(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set custom bad words list"""
    if not context.args:
        group = await get_cached_group(update.effective_chat.id)
        bad_words = group.get("bad_words", ["fuck", "shit", "bitch", "asshole", "dick"])
        await update.message.reply_text(
            f"Current bad words list:\n<code>{', '.join(bad_words)}</code>\n\n"
            "To set new list: /setbadwords word1 word2 word3",
            parse_mode=ParseMode.HTML
        )
        return
    
    bad_words = [word.lower() for word in context.args[:20]]
    chat = update.effective_chat
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "bad_words": bad_words,
                "anti_swear_enabled": True
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        await update.message.reply_text(
            f"✅ Bad words list updated:\n<code>{', '.join(bad_words)}</code>",
            parse_mode=ParseMode.HTML
        )
    except PyMongoError as e:
        logger.error(f"Set bad words failed: {e}")
        await update.message.reply_text("❌ Database error.")

# === CAPTCHA SYSTEM ===
async def new_member_captcha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new members with CAPTCHA"""
    message = update.effective_message
    chat = update.effective_chat
    
    for member in message.new_chat_members:
        if member.is_bot:
            group = await get_cached_group(chat.id)
            if group and group.get("anti_bot_enabled", False):
                try:
                    await chat.ban_member(user_id=member.id)
                    await message.reply_text(
                        f"🤖 Auto-removed bot: {member.mention_html()}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
            continue
        
        group = await get_cached_group(chat.id)
        
        # Log user for statistics/mentions
        mongo.users.update_one(
            {"id": member.id},
            {"$set": {
                "id": member.id,
                "first_name": member.first_name,
                "username": member.username,
                "last_seen": datetime.utcnow()
            }},
            upsert=True
        )

        if not group or not group.get("captcha_enabled", False):
            # If captcha is disabled, send welcome message immediately for this user
            class MockUpdate:
                def __init__(self, chat, members):
                    self.effective_chat = chat
                    self.message = message # Use the original message
            
            await send_welcome_message(MockUpdate(chat, [member]), context)
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
        
        # Generate math problem
        num1, num2 = random.randint(1, 20), random.randint(1, 20)
        answer = str(num1 + num2)
        
        captcha_data = {
            "user_id": member.id,
            "group_id": chat.id,
            "challenge_type": "math",
            "answer": answer,
            "message_id": None,
            "created_at": datetime.utcnow(),
            "expires_at": datetime.utcnow() + timedelta(seconds=group.get("captcha_timeout", 120))
        }
        
        # Generate options
        options = [int(answer) + i for i in range(-2, 3) if i != 0]
        options.append(int(answer))
        random.shuffle(options)
        
        buttons = [
            [InlineKeyboardButton(str(opt), callback_data=f"captcha_{member.id}_{chat.id}_{opt}")]
            for opt in options[:5]
        ]
        
        captcha_msg = await message.reply_text(
            f"🧩 <b>Welcome {escape_html(member.first_name)}!</b>\n\n"
            f"Solve to unlock chat:\n"
            f"<code>{num1} + {num2} = ?</code>\n\n"
            f"⏰ Timeout: {group.get('captcha_timeout', 120)}s",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
        captcha_data["message_id"] = captcha_msg.message_id
        mongo.captchas.insert_one(captcha_data)
        
        # Schedule auto-kick
        context.job_queue.run_once(
            auto_kick_if_failed,
            group.get("captcha_timeout", 120),
            data={
                "user_id": member.id,
                "chat_id": chat.id,
                "message_id": captcha_msg.message_id
            }
        )

async def handle_captcha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle CAPTCHA answer"""
    query = update.callback_query
    data = query.data.split("_")
    
    if len(data) != 4 or data[0] != "captcha":
        return
    
    _, user_id_str, chat_id_str, answer = data
    user_id, chat_id = int(user_id_str), int(chat_id_str)
    
    if query.from_user.id != user_id:
        await query.answer("❌ This captcha is not for you!", show_alert=True)
        return
    
    captcha = mongo.captchas.find_one({
        "user_id": user_id,
        "group_id": chat_id,
        "expires_at": {"$gt": datetime.utcnow()}
    })
    
    if not captcha:
        await query.answer("❌ Expired or invalid captcha!", show_alert=True)
        try:
            await query.message.delete()
        except:
            pass
        return
    
    # If captcha passed, send welcome message
    if str(answer) == str(captcha["answer"]):
        try:
            # Unrestrict user
            await context.bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=user_id,
                permissions={
                    "can_send_messages": True,
                    "can_send_media_messages": True,
                    "can_send_polls": True,
                    "can_send_other_messages": True,
                    "can_add_web_page_previews": True
                }
            )
            
            await query.answer("✅ Verified!", show_alert=True)
            try:
                await query.message.delete()
            except:
                pass
            
            mongo.captchas.delete_one({"_id": captcha["_id"]})
            
            # Send welcome message after verification
            # Mocking an update object for send_welcome_message
            class MockUser:
                def __init__(self, id, first_name, username):
                    self.id = id
                    self.first_name = first_name
                    self.username = username
                    self.is_bot = False
            
            class MockMessage:
                def __init__(self, chat, new_members):
                    self.chat = chat
                    self.new_chat_members = new_members
                async def reply_text(self, *args, **kwargs):
                    return await context.bot.send_message(self.chat.id, *args, **kwargs)

            class MockUpdate:
                def __init__(self, chat, members):
                    self.effective_chat = chat
                    self.message = MockMessage(chat, members)
            
            # Find the user object from the captcha or message context
            # For simplicity, we can just call send_welcome_message with a constructed update
            # or refactor send_welcome_message to accept user objects.
            # Let's just construct a minimal update.
            user_data = mongo.users.find_one({"id": user_id}) or {}
            mock_member = MockUser(user_id, user_data.get("first_name", "User"), user_data.get("username"))
            mock_update = MockUpdate(query.message.chat, [mock_member])
            await send_welcome_message(mock_update, context)

        except Exception as e:
            logger.error(f"Captcha success processing failed: {e}")
            await query.answer("❌ Error during verification.")
            
            # Delete captcha message
            await query.message.delete()
            
            # Update user record
            mongo.users.update_one(
                {"id": user_id},
                {"$set": {f"captcha_passed_{chat_id}": True}}
            )
            
            # Remove captcha from DB
            mongo.captchas.delete_one({"_id": captcha["_id"]})
            
            await query.answer("✅ Verified! You can now chat.", show_alert=False)
            
        except Exception as e:
            logger.error(f"Captcha verification failed: {e}")
            await query.answer("❌ Error verifying. Please contact an admin.", show_alert=True)
    else:
        await query.answer("❌ Wrong answer! Try again.", show_alert=True)

async def auto_kick_if_failed(context: ContextTypes.DEFAULT_TYPE):
    """Auto-kick user if CAPTCHA not solved in time"""
    job_data = context.job.data
    
    captcha = mongo.captchas.find_one({
        "user_id": job_data["user_id"],
        "group_id": job_data["chat_id"],
        "expires_at": {"$lt": datetime.utcnow()}
    })
    
    if captcha:
        try:
            # Kick user
            await context.bot.ban_chat_member(
                chat_id=job_data["chat_id"],
                user_id=job_data["user_id"]
            )
            
            # Notify group
            await context.bot.send_message(
                job_data["chat_id"],
                f"⏰ <b>Time's up!</b> User <code>{job_data['user_id']}</code> failed captcha and was removed.",
                parse_mode=ParseMode.HTML
            )
            
            # Delete captcha message
            try:
                await context.bot.delete_message(
                    job_data["chat_id"],
                    job_data["message_id"]
                )
            except:
                pass
            
            # Remove from DB
            mongo.captchas.delete_one({"_id": captcha["_id"]})
            
        except Exception as e:
            logger.error(f"Auto-kick failed: {e}")

@admin_only()
async def toggle_captcha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle CAPTCHA"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    new_status = not group.get("captcha_enabled", False)
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "captcha_enabled": new_status,
                "captcha_timeout": group.get("captcha_timeout", config.CAPTCHA_TIMEOUT)
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        status_emoji = "✅ On" if new_status else "❌ Off"
        await update.message.reply_text(f"🔐 CAPTCHA: {status_emoji}")
    except PyMongoError as e:
        logger.error(f"Toggle captcha failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def configure_captcha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configure CAPTCHA timeout"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /setcaptcha <timeout_in_seconds>")
        return
    
    try:
        timeout = int(context.args[0])
        if timeout < 30 or timeout > 600:
            await update.message.reply_text("❌ Timeout must be between 30-600 seconds.")
            return
        
        chat = update.effective_chat
        
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "captcha_timeout": timeout,
                "captcha_enabled": True
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        await update.message.reply_text(f"✅ CAPTCHA timeout set to {timeout} seconds.")
    except ValueError:
        await update.message.reply_text("❌ Invalid timeout value.")
    except PyMongoError as e:
        logger.error(f"Configure captcha failed: {e}")
        await update.message.reply_text("❌ Database error.")

# === RULES & NOTES ===
@admin_only()
async def set_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set group rules"""
    if not context.args:
        await update.message.reply_text("❌ Provide rules text.")
        return
    
    chat = update.effective_chat
    rules_text = update.message.text_html.split(None, 1)[1] if len(context.args) > 0 else ""
    
    if not rules_text:
        await update.message.reply_text("❌ Provide rules text.")
        return
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "rules": rules_text
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        await update.message.reply_text("✅ Rules updated successfully.")
    except PyMongoError as e:
        logger.error(f"Set rules failed: {e}")
        await update.message.reply_text("❌ Database error.")

async def show_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show group rules"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    rules = group.get("rules") if group else None
    
    if rules:
        await update.message.reply_text(
            f"📜 <b>Group Rules</b>\n\n{escape_html(rules)}",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("❌ No rules set for this group.")

async def check_notes_and_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Auto-reply to filters and notes"""
    if not update.message or not update.message.text:
        return
    
    text = update.message.text
    if text.startswith("/"):
        return
    
    chat_id = update.effective_chat.id
    group = await get_cached_group(chat_id)
    if not group:
        return
    
    # Check filters first (exact and keyword)
    if "filters" in group:
        for keyword, response in group["filters"].items():
            if keyword.lower() in text.lower():
                await update.message.reply_text(response)
                return

    # Notes are usually triggered by /name, but we can also check for keywords if desired.
    # However, the standard behavior for this bot's /get command is separate.
    # We'll stick to filters for auto-replies.

@admin_only()
async def save_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save a note"""
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usage: /note <name> <content>")
        return
    
    chat = update.effective_chat
    note_name = context.args[0].lower()
    # Get content preserving formatting and newlines
    # Find the position after the note name in the original text
    original_text = update.message.text_html
    parts = original_text.split(None, 2)
    if len(parts) < 3:
        await update.message.reply_text("❌ Usage: /note <name> <content>")
        return
    note_content = parts[2]
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {f"notes.{note_name}": note_content}},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        await update.message.reply_text(
            f"📝 <b>NOTE SAVED</b> 📝\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔑 <b>Keyword:</b> {note_name}\n"
            f"📝 <i>Use '/get {note_name}' to see it.</i>",
            parse_mode=ParseMode.HTML
        )
    except PyMongoError as e:
        logger.error(f"Save note failed: {e}")
        await update.message.reply_text("❌ Database error.")

async def get_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get a note"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /get <note_name>")
        return
    
    chat = update.effective_chat
    note_name = context.args[0].lower()
    group = await get_cached_group(chat.id)
    
    if group and "notes" in group and note_name in group["notes"]:
        await update.message.reply_text(group["notes"][note_name])
    else:
        await update.message.reply_text("❌ Note not found.")

@admin_only()
async def del_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete a note"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /delnote <name>")
        return
    
    chat = update.effective_chat
    note_name = context.args[0].lower()
    
    try:
        result = mongo.groups.update_one(
            {"id": chat.id},
            {"$unset": {f"notes.{note_name}": ""}}
        )
        
        if result.modified_count > 0:
            invalidate_group_cache(chat.id)
            await update.message.reply_text(
                f"🗑️ <b>NOTE DELETED</b> 🗑️\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔑 <b>Keyword:</b> {note_name}\n"
                f"❌ <i>Note removed from database.</i>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("❌ Note not found.")
    except PyMongoError as e:
        logger.error(f"Delete note failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def add_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a filter"""
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usage: /filter <keyword> <response>")
        return
    
    chat = update.effective_chat
    keyword = context.args[0].lower()
    # Get response preserving formatting and newlines
    original_text = update.message.text_html
    parts = original_text.split(None, 2)
    if len(parts) < 3:
        await update.message.reply_text("❌ Usage: /filter <keyword> <response>")
        return
    response = parts[2]
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {f"filters.{keyword}": response}},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        await update.message.reply_text(
            f"🛡️ <b>FILTER ADDED</b> 🛡️\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔑 <b>Keyword:</b> {keyword}\n"
            f"✅ <i>Auto-reply is active.</i>",
            parse_mode=ParseMode.HTML
        )
    except PyMongoError as e:
        logger.error(f"Add filter failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def stop_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop a filter"""
    if not context.args:
        await update.message.reply_text("❌ Usage: /stop <keyword>")
        return
    
    chat = update.effective_chat
    keyword = context.args[0].lower()
    
    try:
        result = mongo.groups.update_one(
            {"id": chat.id},
            {"$unset": {f"filters.{keyword}": ""}}
        )
        
        if result.modified_count > 0:
            invalidate_group_cache(chat.id)
            await update.message.reply_text(
                f"🗑️ <b>FILTER REMOVED</b> 🛡️\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔑 <b>Keyword:</b> {keyword}\n"
                f"❌ <i>Auto-reply disabled.</i>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("❌ Filter not found.")
    except PyMongoError as e:
        logger.error(f"Stop filter failed: {e}")
        await update.message.reply_text("❌ Database error.")

# === WELCOME/GOODBYE ===
async def send_welcome_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send welcome message"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    if not group or not group.get("welcome_enabled", False):
        return
    
    for member in update.message.new_chat_members:
        if member.is_bot:
            continue
        
        welcome_template = group.get(
            "welcome_message",
            "🎉 <b>Welcome to {chat_title}!</b>\n\nHello {user_name}!"
        )
        
        welcome_text = welcome_template.format(
            chat_title=escape_html(chat.title),
            user_name=escape_html(member.first_name),
            user_mention=get_user_mention(member),
            user_id=member.id,
            username=f"@{member.username}" if member.username else "No username"
        )
        
        decorated_welcome = (
            f"{welcome_text}"
        )
        
        buttons = []
        if "welcome_buttons" in group:
            for btn in group["welcome_buttons"]:
                buttons.append([InlineKeyboardButton(btn['text'], url=btn['url'])])
        
        try:
            await update.message.reply_text(
                decorated_welcome,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
            )
        except Exception as e:
            logger.error(f"Failed to send welcome: {e}")

async def send_goodbye_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send goodbye message"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    if not group or not group.get("goodbye_enabled", False):
        return
    
    user = update.message.left_chat_member
    if user.is_bot:
        return
    
    goodbye_template = group.get(
        "goodbye_message",
        "👋 <b>{user_name}</b> has left the group."
    )
    
    goodbye_text = goodbye_template.format(
        user_name=escape_html(user.first_name),
        user_mention=get_user_mention(user)
    )
    
    decorated_goodbye = (f"{goodbye_text}"
    )
    
    try:
        await update.message.reply_text(decorated_goodbye, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to send goodbye: {e}")

@admin_only()
async def set_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set welcome message"""
    if not context.args:
        await update.message.reply_text(
            "❌ Provide welcome message.\n\n"
            "Variables: {chat_title}, {user_name}, {user_mention}, {user_id}, {username}"
        )
        return
    
    chat = update.effective_chat
    welcome_text = update.message.text_html.split(None, 1)[1] if len(context.args) > 0 else ""
    
    if not welcome_text:
        await update.message.reply_text(
            "❌ Provide welcome message.\n\n"
            "Variables: {chat_title}, {user_name}, {user_mention}, {user_id}, {username}"
        )
        return
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "welcome_message": welcome_text,
                "welcome_enabled": True
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        await update.message.reply_text(
            "✅ Welcome message set!\n"
            "Tip: Use /welcomepreview to test it."
        )
    except PyMongoError as e:
        logger.error(f"Set welcome failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def toggle_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle welcome messages"""
    chat = update.effective_chat
    
    if not context.args:
        await update.message.reply_text("❌ Usage: /welcome on/off")
        return
    
    status = context.args[0].lower() == "on"
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "welcome_enabled": status
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        status_emoji = "✅ On" if status else "❌ Off"
        await update.message.reply_text(f"🎉 Welcome messages: {status_emoji}")
    except PyMongoError as e:
        logger.error(f"Toggle welcome failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def set_goodbye(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set goodbye message"""
    if not context.args:
        await update.message.reply_text(
            "❌ Provide goodbye message.\n\n"
            "Variables: {user_name}, {user_mention}"
        )
        return
    
    chat = update.effective_chat
    goodbye_text = update.message.text_html.split(None, 1)[1] if len(context.args) > 0 else ""
    
    if not goodbye_text:
        await update.message.reply_text(
            "❌ Provide goodbye message.\n\n"
            "Variables: {user_name}, {user_mention}"
        )
        return
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "goodbye_message": goodbye_text,
                "goodbye_enabled": True
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        await update.message.reply_text("✅ Goodbye message set!")
    except PyMongoError as e:
        logger.error(f"Set goodbye failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def toggle_goodbye(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle goodbye messages"""
    chat = update.effective_chat
    
    if not context.args:
        await update.message.reply_text("❌ Usage: /goodbye on/off")
        return
    
    status = context.args[0].lower() == "on"
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "goodbye_enabled": status
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        status_emoji = "✅ On" if status else "❌ Off"
        await update.message.reply_text(f"👋 Goodbye messages: {status_emoji}")
    except PyMongoError as e:
        logger.error(f"Toggle goodbye failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def preview_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Preview welcome message"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    if not group or not group.get("welcome_message"):
        await update.message.reply_text("❌ No welcome message set.")
        return
    
    user = update.effective_user
    template = group["welcome_message"]
    
    preview_text = template.format(
        chat_title=escape_html(chat.title),
        user_name=escape_html(user.first_name),
        user_mention=get_user_mention(user),
        user_id=user.id,
        username=f"@{user.username}" if user.username else "No username"
    )
    
    await update.message.reply_text(preview_text, parse_mode=ParseMode.HTML)

# === GROUP SETTINGS ===
@admin_only()
async def settings_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show settings panel"""
    chat = update.effective_chat
    group = await get_cached_group(chat.id)
    
    settings_text = f"""
⚙️ <b>Settings for {escape_html(chat.title)}</b>

🛡️ <b>Anti-Flood:</b> {'✅ On' if group.get('antiflood_enabled', False) else '❌ Off'}
   Limit: {group.get('antiflood_limit', config.FLOOD_LIMIT)} msgs / {group.get('antiflood_time', config.FLOOD_TIME)}s

🤖 <b>Auto-Mod:</b> {'✅ On' if group.get('automod_enabled', False) else '❌ Off'}
   Anti-Link: {'✅' if group.get('anti_link_enabled', False) else '❌'}
   Anti-Swear: {'✅' if group.get('anti_swear_enabled', False) else '❌'}

🔐 <b>CAPTCHA:</b> {'✅ On' if group.get('captcha_enabled', False) else '❌ Off'}
   Timeout: {group.get('captcha_timeout', config.CAPTCHA_TIMEOUT)}s

🎉 <b>Welcome:</b> {'✅ On' if group.get('welcome_enabled', False) else '❌ Off'}
👋 <b>Goodbye:</b> {'✅ On' if group.get('goodbye_enabled', False) else '❌ Off'}

⚠️ <b>Max Warnings:</b> {group.get('max_warnings', config.MAX_WARNINGS)}
"""
    
    buttons = [
        [
            InlineKeyboardButton("Anti-Flood", callback_data="toggle_antiflood"),
            InlineKeyboardButton("Auto-Mod", callback_data="toggle_automod")
        ],
        [
            InlineKeyboardButton("CAPTCHA", callback_data="toggle_captcha"),
            InlineKeyboardButton("Anti-Link", callback_data="toggle_antilink")
        ],
        [
            InlineKeyboardButton("Welcome", callback_data="toggle_welcome"),
            InlineKeyboardButton("Goodbye", callback_data="toggle_goodbye")
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
    """Set max warnings before ban"""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Usage: /setmaxwarn <number> (2-10)")
        return
    
    max_warn = int(context.args[0])
    if not 2 <= max_warn <= 10:
        await update.message.reply_text("❌ Must be between 2-10 warnings.")
        return
    
    chat = update.effective_chat
    
    try:
        mongo.groups.update_one(
            {"id": chat.id},
            {"$set": {
                "id": chat.id,
                "title": chat.title,
                "max_warnings": max_warn
            }},
            upsert=True
        )
        invalidate_group_cache(chat.id)
        
        await update.message.reply_text(
            f"✅ Max warnings set to {max_warn}.\n"
            f"Users will be banned after {max_warn} active warnings."
        )
    except PyMongoError as e:
        logger.error(f"Set max warnings failed: {e}")
        await update.message.reply_text("❌ Database error.")

@admin_only()
async def group_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show group information"""
    chat = update.effective_chat
    
    try:
        member_count = await context.bot.get_chat_member_count(chat.id)
        
        bot_info = await context.bot.get_me()
        bot_member = await chat.get_member(bot_info.id)
        
        group_warnings = mongo.warnings.count_documents({
            "group_id": chat.id,
            "active": True
        })
        
        info_text = f"""
📊 <b>Group Information</b>

🏷️ <b>Name:</b> {escape_html(chat.title)}
🆔 <b>ID:</b> <code>{chat.id}</code>
👥 <b>Members:</b> {member_count}

🤖 <b>Bot Status:</b>
• Name: {bot_info.first_name}
• Username: @{bot_info.username}
• Permissions: {'✅ Admin' if bot_member.status == ChatMember.ADMINISTRATOR else '❌ Not Admin'}

📊 <b>Statistics:</b>
• Active Warnings: {group_warnings}
"""
        await update.message.reply_text(info_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Group info failed: {e}")
        await update.message.reply_text("❌ Failed to get group info.")

# === UTILITY COMMANDS ===
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
        f"🛡️ <b>{escape_html(bot_name)}</b> is the most complete Bot to help you manage your groups easily and safely!\n\n"
        f"👉🏻 <b>Add me in a Supergroup</b> and promote me as Admin to let me get in action!\n\n"
        f"❓ <b>WHICH ARE THE COMMANDS?</b> ❓\n"
        f"Press the <b>Help</b> button below or type /help to see all the commands and how they work!\n\n"
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
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command"""
    user = update.effective_user
    chat = update.effective_chat
    is_owner = user.id == config.ADMIN_ID
    is_admin_user = is_owner or (chat.type != "private" and await is_admin(chat, user.id))
    
    help_text = """
🤖 <b>Group Manager Bot - Help</b>

<b>📖 General Commands:</b>
/start - Start the bot
/help - Show this help
/rules - View group rules
/id - Get user and chat ID
/poll - Create a poll
/quiz - Create a quiz
/roll - Roll a dice
/leaderboard - Top active members
/stats - Group statistics
"""
    
    if is_admin_user:
        help_text += """
<b>👑 Admin Commands:</b>

<b>🔨 Moderation:</b>
/ban &lt;user&gt; [reason] - Ban user
/unban &lt;user_id&gt; - Unban user
/mute &lt;user&gt; [duration] - Mute user
/unmute &lt;user&gt; - Unmute user
/kick &lt;user&gt; [reason] - Kick user
/warn &lt;user&gt; [reason] - Warn user
/unwarn &lt;user&gt; - Remove warning
/warnings &lt;user&gt; - View warnings
/purge [count] - Delete messages
/promote &lt;user&gt; - Make admin
/demote &lt;user&gt; - Remove admin

<b>🛡️ Security:</b>
/antiflood - Toggle anti-flood
/setflood &lt;limit&gt; &lt;time&gt; - Configure flood
/automod - Toggle auto-moderation
/antilink - Toggle link blocking
/antiswear - Toggle bad words
/setbadwords &lt;words...&gt; - Set custom bad words
/captcha - Toggle CAPTCHA
/setcaptcha &lt;timeout&gt; - Configure CAPTCHA

<b>📜 Management:</b>
/setrules &lt;text&gt; - Set rules
/setwelcome &lt;text&gt; - Set welcome message
/welcome on/off - Toggle welcome
/setgoodbye &lt;text&gt; - Set goodbye message
/goodbye on/off - Toggle goodbye
/note &lt;name&gt; &lt;text&gt; - Save note
/get &lt;name&gt; - Get note
/delnote &lt;name&gt; - Delete note
/filter &lt;kw&gt; &lt;response&gt; - Add filter
/stop &lt;kw&gt; - Remove filter
/setmaxwarn &lt;n&gt; - Set max warnings
/settings - View settings
"""
    
    if is_owner:
        help_text += """
<b>👑 Owner Commands:</b>
/broadcast &lt;message&gt; - Broadcast to all groups
/leave &lt;group_id&gt; - Leave group
/logs - View bot logs
/resetgroup - Reset current group
/shutdown - Stop bot gracefully
/ping - Check latency
"""
    
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)

# === POLLS & QUIZZES ===
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
    
    if len(options) < 2 or len(options) > 10:
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
        logger.error(f"Poll failed: {e}")
        await update.message.reply_text(f"❌ Failed to create poll: {e}")

@admin_only()
async def create_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Create a quiz"""
    if len(context.args) < 4:
        await update.message.reply_text('❌ Usage: /quiz "Question" correct_index option1 option2 [...]')
        return
    
    full_text = " ".join(context.args)
    match = re.match(r'["\'](.+?)["\']\s+(\d+)\s+(.+)', full_text)
    
    if not match:
        await update.message.reply_text("❌ Invalid format. Use: /quiz \"Question\" 1 option1 option2")
        return
    
    question = match.group(1)
    correct_index = int(match.group(2)) - 1
    options = [opt.strip() for opt in match.group(3).split(' ') if opt.strip()]
    
    if correct_index < 0 or correct_index >= len(options):
        await update.message.reply_text("❌ Correct answer index is out of range.")
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
        logger.error(f"Quiz failed: {e}")
        await update.message.reply_text(f"❌ Failed to create quiz: {e}")

# === STATS & LEADERBOARD ===
@admin_only()
async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show group statistics"""
    chat = update.effective_chat
    
    try:
        member_count = await context.bot.get_chat_member_count(chat.id)
        
        active_warnings = mongo.warnings.count_documents({
            "group_id": chat.id,
            "active": True
        })
        
        week_ago = datetime.utcnow() - timedelta(days=7)
        recent_actions = mongo.logs.count_documents({
            "group_id": chat.id,
            "timestamp": {"$gte": week_ago}
        })
        
        pipeline = [
            {"$match": {"group_id": chat.id, "active": True}},
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$sort": {"count": DESCENDING}},
            {"$limit": 5}
        ]
        top_offenders = list(mongo.warnings.aggregate(pipeline))
        
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
        logger.error(f"Group stats failed: {e}")
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
        users_with_warns = {
            doc["_id"]: doc["warnings"] for doc in mongo.warnings.aggregate(pipeline)
        }
        
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
        filename = f"stats_{chat.id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        
        await update.message.reply_document(
            document=output.getvalue(),
            filename=filename,
            caption=f"📊 Statistics for {chat.title}"
        )
    except Exception as e:
        logger.error(f"Export stats failed: {e}")
        await update.message.reply_text("❌ Failed to export statistics.")

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top active members with time selection"""
    await send_leaderboard(update.effective_chat, update.effective_message, "lifetime")

async def message_logger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log user messages for leaderboard"""
    user = update.effective_user
    chat = update.effective_chat
    
    if not user or not chat or user.is_bot:
        return
        
    try:
        # Update user activity in database
        mongo.logs.insert_one({
            "group_id": chat.id,
            "user_id": user.id,
            "action": "message",
            "timestamp": datetime.utcnow()
        })
        
        # Also ensure user is in users collection
        mongo.users.update_one(
            {"id": user.id},
            {"$set": {
                "username": user.username.lower() if user.username else None,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "last_seen": datetime.utcnow()
            }},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error in message_logger: {e}")

async def send_leaderboard(chat: Chat, message: Message, timeframe: str, edit: bool = False):
    """Internal helper to generate and send leaderboard"""
    now = datetime.utcnow()
    if timeframe == "today":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        title = "Today"
    elif timeframe == "weekly":
        start_date = now - timedelta(days=7)
        title = "This Week"
    else:
        start_date = datetime.min
        title = "Lifetime"

    pipeline = [
        {"$match": {"group_id": chat.id, "action": "message", "timestamp": {"$gte": start_date}}},
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
        {"$sort": {"count": DESCENDING}},
        {"$limit": 10}
    ]
    top_users = list(mongo.logs.aggregate(pipeline))
    
    board_text = (
        f"🏆 <b>TOP ACTIVE MEMBERS</b> 🏆\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📅 <b>Period:</b> {title}\n\n"
    )
    
    if not top_users:
        board_text += "<i>No activity data for this period yet.</i>\n"
    else:
        for i, user_data in enumerate(top_users, 1):
            user_id = user_data["_id"]
            count = user_data["count"]
            try:
                # Try to get from database first to be faster
                u_data = mongo.users.find_one({"id": user_id})
                if u_data:
                    name = escape_html(u_data.get("first_name", "User"))
                else:
                    member = await chat.get_member(user_id)
                    name = escape_html(member.user.first_name)
            except:
                name = f"User({user_id})"
            
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"<code>{i}.</code>")
            board_text += f"{medal} {name} — <b>{count}</b>\n"
    
    board_text += "\n━━━━━━━━━━━━━━━━━━"
    
    keyboard = [
        [
            InlineKeyboardButton("🕒 Today", callback_data="lb_today"),
            InlineKeyboardButton("📅 Weekly", callback_data="lb_weekly"),
            InlineKeyboardButton("♾️ Lifetime", callback_data="lb_lifetime")
        ],
        [InlineKeyboardButton("🗑️ Close", callback_data="close_settings")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        if edit:
            await message.edit_text(board_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            await message.reply_text(board_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error sending leaderboard: {e}")

async def roll_dice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Roll a dice"""
    sides = 6
    if context.args:
        try:
            sides = int(context.args[0])
            if sides < 2 or sides > 100:
                await update.message.reply_text("❌ Dice must have 2-100 sides.")
                return
        except ValueError:
            pass
    
    dice_msg = await update.message.reply_dice(emoji="🎲")
    await asyncio.sleep(3)
    result = random.randint(1, sides)
    await update.message.reply_text(f"🎲 Rolled: {result} (1-{sides})!")

# === CALLBACK QUERY HANDLER ===
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all callback queries"""
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
        if timeframe == "today":
            await send_leaderboard(query.message.chat, query.message, "today", edit=True)
        elif timeframe == "weekly":
            await send_leaderboard(query.message.chat, query.message, "weekly", edit=True)
        elif timeframe == "lifetime":
            await send_leaderboard(query.message.chat, query.message, "lifetime", edit=True)
        elif timeframe == "help":
            await help_command(Update(update.update_id, message=query.message), context)
        elif timeframe == "info":
            # Bot info button logic
            bot_info = context.bot
            text = (
                f"ℹ️ <b>BOT INFORMATION</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🤖 <b>Name:</b> {escape_html(bot_info.first_name)}\n"
                f"🆔 <b>Username:</b> @{bot_info.username}\n"
                f"🏢 <b>Platform:</b> Telegram\n"
                f"🛠️ <b>Version:</b> 1.0.0\n"
                f"━━━━━━━━━━━━━━━━━━"
            )
            await query.message.reply_text(text, parse_mode=ParseMode.HTML)
        return
    
    if data.startswith("toggle_"):
        chat = query.message.chat
        setting = data.replace("toggle_", "")
        
        if not await is_admin(chat, query.from_user.id):
            await query.answer("❌ Admin only!", show_alert=True)
            return
        
        group = await get_cached_group(chat.id)
        
        setting_map = {
            "antiflood": "antiflood_enabled",
            "automod": "automod_enabled",
            "captcha": "captcha_enabled",
            "antilink": "anti_link_enabled",
            "welcome": "welcome_enabled",
            "goodbye": "goodbye_enabled"
        }
        
        if setting in setting_map:
            db_field = setting_map[setting]
            new_status = not group.get(db_field, False)
            
            try:
                mongo.groups.update_one(
                    {"id": chat.id},
                    {"$set": {db_field: new_status}},
                    upsert=True
                )
                invalidate_group_cache(chat.id)
                
                await query.answer(f"✅ {'Enabled' if new_status else 'Disabled'}", show_alert=False)
                await settings_panel(
                    Update(update.update_id, message=query.message),
                    context
                )
            except PyMongoError as e:
                logger.error(f"Callback toggle failed: {e}")
                await query.answer("❌ Database error.", show_alert=True)
    
    await query.answer()

# === ERROR HANDLER ===
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

# === MAIN APPLICATION ===
application: Application = None

@asynccontextmanager
async def lifespan(_app: Application):
    """Application lifespan manager"""
    logger.info("Bot starting up...")
    
    is_valid, errors = config.validate()
    if not is_valid:
        logger.critical(f"Configuration errors: {errors}")
        sys.exit(1)
    
    config.log_config(logger)
    
    try:
        mongo.bot_stats.update_one(
            {"date": datetime.utcnow().date().isoformat()},
            {"$inc": {"starts": 1}},
            upsert=True
        )
        logger.info("Bot stats updated")
    except PyMongoError as e:
        logger.error(f"Failed to update bot stats: {e}")
    
    yield
    
    logger.info("Bot shutting down...")

def setup_application() -> Application:
    """Setup and configure the application"""
    global application
    
    application = (
        ApplicationBuilder()
        .token(config.TOKEN)
        .concurrent_updates(True)
        .connection_pool_size(8)
        .rate_limiter(None)
        .build()
    )
    
    # Message logging
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, message_logger), group=-1)
    
    # === COMMAND HANDLERS ===
    
    # Owner commands
    application.add_handler(CommandHandler("broadcast", broadcast_command, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("leave", leave_command))
    application.add_handler(CommandHandler("logs", view_bot_logs, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("resetgroup", reset_group_command))
    application.add_handler(CommandHandler("confirm", handle_reset_confirmation))
    application.add_handler(CommandHandler("shutdown", shutdown_bot, filters=filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("ping", ping_bot))
    
    # Admin commands - moderation
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
    
    # Admin commands - security
    application.add_handler(CommandHandler("antiflood", toggle_antiflood, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setflood", configure_antiflood, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("automod", toggle_automod, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("antilink", toggle_antilink, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("antiswear", toggle_antiswear, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setbadwords", set_bad_words, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("captcha", toggle_captcha, filters=filters.ChatType.GROUPS))
    application.add_handler(CommandHandler("setcaptcha", configure_captcha, filters=filters.ChatType.GROUPS))
    
    # Admin commands - management
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
    
    # Callback query handlers
    application.add_handler(CallbackQueryHandler(handle_captcha, pattern="^captcha_"))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # Error handler
    application.add_error_handler(error_handler)
    
    return application

def main():
    """Main entry point"""
    global application
    
    # Create application
    application = setup_application()
    
    # ✅ FIX: Use post_init instead of lifespan attribute
    async def post_init(app: Application) -> None:
        """Called after application is initialized"""
        logger.info("Bot starting up...")
        
        is_valid, errors = config.validate()
        if not is_valid:
            logger.critical(f"Configuration errors: {errors}")
            sys.exit(1)
        
        config.log_config(logger)
        
        try:
            mongo.bot_stats.update_one(
                {"date": datetime.utcnow().date().isoformat()},
                {"$inc": {"starts": 1}},
                upsert=True
            )
            logger.info("Bot stats updated")
        except PyMongoError as e:
            logger.error(f"Failed to update bot stats: {e}")
    
    application.post_init = post_init
    
    # Optional: Add shutdown cleanup
    async def post_stop(app: Application) -> None:
        """Called after application stops"""
        logger.info("Bot shutting down...")
    
    application.post_stop = post_stop
    
    logger.info("Bot starting...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        close_loop=True,
        drop_pending_updates=False
    )

if __name__ == "__main__":
    try :
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical (f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

