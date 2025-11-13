import os
import logging
import aiohttp
from typing import Optional
from services.database import UserDatabase

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
CHANNEL_INVITE_LINK = os.getenv('CHANNEL_INVITE_LINK', 'https://t.me/your_channel')

user_db = UserDatabase()
user_states = {}


async def send_message(chat_id: int, text: str, parse_mode: str = "HTML"):
    """Send a message to a Telegram user"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode
        }) as response:
            return await response.json()


async def handle_telegram_update(update: dict):
    """
    Handle incoming Telegram messages for auto-onboarding
    
    Flow:
    1. User messages bot with /start
    2. Bot asks for their Gumroad email
    3. User provides email
    4. Bot auto-links Telegram ID to email
    5. Bot sends them the channel invite link
    """
    try:
        message = update.get('message', {})
        chat_id = message.get('chat', {}).get('id')
        user_id = message.get('from', {}).get('id')
        username = message.get('from', {}).get('username', 'User')
        text = message.get('text', '').strip()
        
        if not chat_id or not user_id:
            return
        
        # Handle /start command OR any first message
        if text.startswith('/start') or user_id not in user_states:
            user_states[user_id] = 'waiting_for_email'
            
            await send_message(
                chat_id,
                f"Hey {username}! 👋\n\n"
                f"Just send me the email you used to purchase and I'll get you access to the channel right away."
            )
            logger.info(f"User {user_id} ({username}) started onboarding")
            return
        
        # Handle email submission
        if user_states.get(user_id) == 'waiting_for_email':
            email = text.lower()
            
            # Basic email validation
            if '@' not in email or '.' not in email:
                await send_message(
                    chat_id,
                    "Hmm, that doesn't look like an email address. Can you double-check and send it again?"
                )
                return
            
            # Auto-link user
            user_db.link_user(email, user_id)
            user_states[user_id] = 'completed'
            
            await send_message(
                chat_id,
                f"Perfect! ✅\n\n"
                f"Here's your invite link to the premium channel:\n{CHANNEL_INVITE_LINK}\n\n"
                f"Welcome aboard! 🎉"
            )
            
            logger.info(f"Successfully linked {email} -> Telegram ID {user_id}")
            return
        
        # Unknown state - show help
        await send_message(
            chat_id,
            "Hey! Just send me the email you used to purchase and I'll get you access."
        )
        
    except Exception as e:
        logger.error(f"Error handling Telegram update: {str(e)}")
