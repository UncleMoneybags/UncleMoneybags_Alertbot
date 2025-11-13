import logging
import aiohttp
from typing import Optional

logger = logging.getLogger(__name__)


class TelegramBot:
    """Telegram Bot API wrapper for managing chat members"""
    
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
    
    async def remove_user(self, user_id: int) -> bool:
        """
        Remove user from Telegram group/channel
        
        Args:
            user_id: Telegram user ID to remove
            
        Returns:
            True if successful, False otherwise
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/banChatMember",
                    json={
                        "chat_id": self.chat_id,
                        "user_id": user_id
                    }
                ) as response:
                    result = await response.json()
                    
                    if result.get('ok'):
                        logger.info(f"Banned user {user_id} from chat {self.chat_id}")
                        
                        await session.post(
                            f"{self.base_url}/unbanChatMember",
                            json={
                                "chat_id": self.chat_id,
                                "user_id": user_id,
                                "only_if_banned": True
                            }
                        )
                        logger.info(f"Unbanned user {user_id} (so they can rejoin later)")
                        return True
                    else:
                        logger.error(f"Failed to ban user {user_id}: {result}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error removing user {user_id}: {str(e)}")
            return False
    
    async def get_chat_info(self) -> Optional[dict]:
        """Get information about the chat"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}/getChat",
                    params={"chat_id": self.chat_id}
                ) as response:
                    result = await response.json()
                    
                    if result.get('ok'):
                        return result.get('result')
                    else:
                        logger.error(f"Failed to get chat info: {result}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error getting chat info: {str(e)}")
            return None
