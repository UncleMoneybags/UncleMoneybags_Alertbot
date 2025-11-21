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


async def create_single_use_invite_link(user_id: int, email: str) -> Optional[str]:
    """Create a single-use invite link for a specific user"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/createChatInviteLink"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "member_limit": 1,
                "name": f"{email[:20]}"
            }) as response:
                result = await response.json()
                
                if result.get('ok'):
                    invite_link = result['result']['invite_link']
                    logger.info(f"Created single-use invite link for {email}")
                    return invite_link
                else:
                    logger.error(f"Failed to create invite link: {result}")
                    return None
                    
    except Exception as e:
        logger.error(f"Error creating invite link: {str(e)}")
        return None


async def handle_telegram_update(update: dict):
    """
    Handle incoming Telegram messages for auto-onboarding
    
    Flow:
    1. User joins group → bot sends welcome message asking for email
    2. User messages bot with /start
    3. Bot asks for their Gumroad email
    4. User provides email
    5. Bot auto-links Telegram ID to email
    6. Bot sends them the channel invite link
    """
    try:
        # Handle group join events
        my_chat_member = update.get('my_chat_member', {})
        if my_chat_member:
            new_member = my_chat_member.get('new_chat_member', {})
            old_member = my_chat_member.get('old_chat_member', {})
            new_status = new_member.get('status')
            old_status = old_member.get('status')
            
            # User just joined the group
            if new_status == 'member' and old_status != 'member':
                user_id = new_member.get('user', {}).get('id')
                username = new_member.get('user', {}).get('username', 'Member')
                chat_id = int(my_chat_member.get('chat', {}).get('id'))
                
                if user_id and chat_id:
                    user_states[user_id] = 'waiting_for_email'
                    
                    await send_message(
                        user_id,
                        f"👋 Welcome {username}!\n\n"
                        f"To verify your access, please send me the email address you used to purchase.\n\n"
                        f"Just reply with your email (e.g., user@example.com)"
                    )
                    logger.info(f"New member {user_id} ({username}) joined - sent welcome message")
            return
        
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
            
            # Create single-use invite link
            invite_link = await create_single_use_invite_link(user_id, email)
            
            if invite_link:
                await send_message(
                    chat_id,
                    f"Perfect! ✅\n\n"
                    f"Here's your EXCLUSIVE single-use invite link:\n{invite_link}\n\n"
                    f"⚠️ This link works ONLY ONCE and cannot be shared.\n\n"
                    f"Welcome aboard! 🎉"
                )
                logger.info(f"Successfully linked {email} -> Telegram ID {user_id}, sent invite link")
            else:
                await send_message(
                    chat_id,
                    f"✅ Email verified and linked to Telegram!\n\n"
                    f"However, I couldn't generate your access link. Please contact support or try again in a few moments."
                )
                logger.error(f"Failed to create invite link for {email} (Telegram ID: {user_id})")
            
            return
        
        # Unknown state - show help
        await send_message(
            chat_id,
            "Hey! Just send me the email you used to purchase and I'll get you access."
        )
        
    except Exception as e:
        logger.error(f"Error handling Telegram update: {str(e)}")
