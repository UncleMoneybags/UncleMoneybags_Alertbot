import os
import hmac
import hashlib
import json
import logging
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
from services.telegram import TelegramBot
from services.database import UserDatabase
from bot_handler import handle_telegram_update

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Gumroad-Telegram Subscription Manager")

GUMROAD_WEBHOOK_SECRET = os.getenv('GUMROAD_WEBHOOK_SECRET') or os.getenv('GUMROAD_SELLER_ID')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not GUMROAD_WEBHOOK_SECRET:
    logger.warning("GUMROAD_WEBHOOK_SECRET or GUMROAD_SELLER_ID not set - webhook verification disabled")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set!")
if not TELEGRAM_CHAT_ID:
    logger.error("TELEGRAM_CHAT_ID not set!")

telegram_bot = TelegramBot(
    TELEGRAM_BOT_TOKEN or "",
    TELEGRAM_CHAT_ID or ""
)
user_db = UserDatabase()


def verify_gumroad_signature(payload: bytes, signature: str) -> bool:
    """Verify Gumroad webhook signature using HMAC-SHA256"""
    if not GUMROAD_WEBHOOK_SECRET:
        logger.warning("Cannot verify signature - GUMROAD_WEBHOOK_SECRET not set")
        return False
    
    expected_signature = hmac.new(
        GUMROAD_WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(signature, expected_signature)


async def process_cancellation(email: str, gumroad_id: str):
    """Process subscription cancellation - remove user from Telegram"""
    try:
        telegram_user_id = user_db.get_telegram_id(email, gumroad_id)
        
        if not telegram_user_id:
            logger.warning(f"No Telegram user found for email={email}, gumroad_id={gumroad_id}")
            return
        
        success = await telegram_bot.remove_user(telegram_user_id)
        
        if success:
            user_db.mark_removed(email, gumroad_id)
            logger.info(f"Successfully removed {email} (Telegram ID: {telegram_user_id}) from group")
        else:
            logger.error(f"Failed to remove {email} from Telegram")
            
    except Exception as e:
        logger.error(f"Error processing cancellation for {email}: {str(e)}")


@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "running",
        "service": "Gumroad-Telegram Integration",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/gumroad/webhook")
async def gumroad_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Handle Gumroad webhook events
    
    Events handled:
    - sale.subscription_cancelled
    - sale.refunded
    - subscriber.deleted
    """
    try:
        payload = await request.body()
        signature = request.headers.get('X-Gumroad-Signature', '')
        
        # Verify signature only if secret is configured
        if GUMROAD_WEBHOOK_SECRET:
            if signature and not verify_gumroad_signature(payload, signature):
                logger.warning("Invalid Gumroad signature")
                raise HTTPException(status_code=401, detail="Invalid signature")
        else:
            logger.warning("⚠️  GUMROAD_WEBHOOK_SECRET not set - webhook running WITHOUT verification (INSECURE)")
        
        data = await request.json()
        event_type = data.get('event')
        sale_data = data.get('sale', {})
        
        logger.info(f"Received Gumroad webhook: {event_type}")
        logger.debug(f"Webhook data: {json.dumps(data, indent=2)}")
        
        if event_type in ['sale.subscription_cancelled', 'sale.refunded', 'subscriber.deleted']:
            email = sale_data.get('email') or sale_data.get('purchaser', {}).get('email')
            gumroad_id = sale_data.get('id') or sale_data.get('purchaser', {}).get('id')
            
            if email:
                background_tasks.add_task(process_cancellation, email, gumroad_id)
                logger.info(f"Queued cancellation processing for {email}")
            else:
                logger.warning(f"No email found in {event_type} event")
        
        return JSONResponse(content={"status": "success"}, status_code=200)
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON payload")
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except Exception as e:
        logger.error(f"Webhook processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/link-user")
async def link_user(request: Request):
    """
    Admin endpoint to link Gumroad email to Telegram user ID
    
    POST body: {"email": "user@example.com", "telegram_id": 123456789, "gumroad_id": "abc123"}
    """
    try:
        data = await request.json()
        email = data.get('email')
        telegram_id = data.get('telegram_id')
        gumroad_id = data.get('gumroad_id')
        
        if not all([email, telegram_id]):
            raise HTTPException(status_code=400, detail="email and telegram_id required")
        
        user_db.link_user(email, telegram_id, gumroad_id)
        logger.info(f"Linked {email} to Telegram ID {telegram_id}")
        
        return {"status": "success", "message": f"Linked {email} to Telegram ID {telegram_id}"}
        
    except Exception as e:
        logger.error(f"Error linking user: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/users")
async def list_users():
    """List all linked users"""
    try:
        users = user_db.get_all_users()
        return {"users": users}
    except Exception as e:
        logger.error(f"Error listing users: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    """
    Handle incoming Telegram messages for auto-onboarding
    
    Users message the bot, provide their email, and get auto-linked
    """
    try:
        update = await request.json()
        await handle_telegram_update(update)
        return JSONResponse(content={"status": "ok"}, status_code=200)
        
    except Exception as e:
        logger.error(f"Telegram webhook error: {str(e)}")
        return JSONResponse(content={"status": "error"}, status_code=200)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=5000,
        reload=True
    )
