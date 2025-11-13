import sqlite3
import logging
from datetime import datetime
from typing import Optional, List, Dict
import threading

logger = logging.getLogger(__name__)


class UserDatabase:
    """SQLite database for storing Gumroad email <-> Telegram user ID mappings"""
    
    def __init__(self, db_path: str = "gumroad_telegram.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()
    
    def _init_db(self):
        """Initialize database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT NOT NULL,
                    telegram_id INTEGER NOT NULL,
                    gumroad_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    removed_at TIMESTAMP,
                    UNIQUE(email)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_email ON user_mappings(email)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_telegram_id ON user_mappings(telegram_id)
            """)
            conn.commit()
            logger.info("Database initialized")
    
    def link_user(self, email: str, telegram_id: int, gumroad_id: Optional[str] = None):
        """Link a Gumroad email to Telegram user ID"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO user_mappings (email, telegram_id, gumroad_id)
                    VALUES (?, ?, ?)
                """, (email, telegram_id, gumroad_id))
                conn.commit()
                logger.info(f"Linked {email} -> Telegram ID {telegram_id}")
    
    def get_telegram_id(self, email: str, gumroad_id: Optional[str] = None) -> Optional[int]:
        """Get Telegram user ID from email or Gumroad ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if gumroad_id:
                cursor.execute(
                    "SELECT telegram_id FROM user_mappings WHERE gumroad_id = ? OR email = ? LIMIT 1",
                    (gumroad_id, email)
                )
            else:
                cursor.execute(
                    "SELECT telegram_id FROM user_mappings WHERE email = ? LIMIT 1",
                    (email,)
                )
            
            result = cursor.fetchone()
            return result[0] if result else None
    
    def mark_removed(self, email: str, gumroad_id: Optional[str] = None):
        """Mark user as removed"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                if gumroad_id:
                    conn.execute(
                        "UPDATE user_mappings SET removed_at = ? WHERE email = ? OR gumroad_id = ?",
                        (datetime.utcnow(), email, gumroad_id)
                    )
                else:
                    conn.execute(
                        "UPDATE user_mappings SET removed_at = ? WHERE email = ?",
                        (datetime.utcnow(), email)
                    )
                conn.commit()
                logger.info(f"Marked {email} as removed")
    
    def get_all_users(self) -> List[Dict]:
        """Get all user mappings"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT email, telegram_id, gumroad_id, created_at, removed_at
                FROM user_mappings
                ORDER BY created_at DESC
            """)
            
            return [dict(row) for row in cursor.fetchall()]
