# 🚀 Quick Setup - Auto-Remove Gumroad Cancellations

## What This Does

When someone cancels their Gumroad subscription, they're **automatically removed** from your Telegram channel. No manual work required.

## Bot Naming Tips

Your bot username **must** end in "bot" (Telegram requirement), but make it feel personal:

- ✅ `@SarahSupportBot` (feels like a person)
- ✅ `@VIPAccessBot` (feels premium)
- ✅ `@TeamHelpBot` (feels like your team)
- ❌ `@MyChannelAutoBot123` (feels robotic)

The messages are conversational - customers won't realize it's automated.

---

## ⚡ Setup (5 minutes)

### 1. **Deploy the Service**

**On Render (same place as your scanner):**

```bash
# Add to your repo
git add gumroad_telegram/
git commit -m "Add auto-removal service"
git push
```

Then in Render dashboard:
- Create **New Web Service**
- Root Directory: `gumroad_telegram`
- Build: `pip install -r requirements.txt`
- Start: `python main.py`
- Add environment variables:
  - `GUMROAD_WEBHOOK_SECRET` (from Gumroad settings)
  - `TELEGRAM_BOT_TOKEN` (your existing bot)
  - `TELEGRAM_CHAT_ID` (your existing channel)
  - `CHANNEL_INVITE_LINK` (your channel invite link, e.g., `https://t.me/+ABC123xyz`)

### 2. **Set Up Gumroad Webhook**

1. Go to [Gumroad Settings > Advanced](https://app.gumroad.com/settings/advanced)
2. Under "Ping URL", enter: `https://your-render-url.onrender.com/gumroad/webhook`
3. Click "Send test ping" ✅

### 3. **Set Up Telegram Bot Webhook**

Run this command (replace with your URL and bot token):

```bash
curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook?url=https://your-render-url.onrender.com/telegram/webhook"
```

### 4. **Update Your Gumroad Product Page**

Add this to your product description (make it feel personal, not automated):

```
🎉 Welcome!

To access your premium Telegram channel:

1. Message @YourBotName on Telegram
2. Send the email you used for this purchase
3. Get instant access!

Note: Access is tied to your active subscription.
```

**They won't know it's automated** - the messages feel conversational and personal.

---

## 🎯 How It Works (Customer Perspective)

**The conversation feels natural:**

1. **Customer buys** → gets email: "Message @SarahSupportBot with your purchase email"
2. **Customer:** `Hey` or `Hi` or anything
3. **Bot:** `Hey John! 👋 Just send me the email you used to purchase and I'll get you access to the channel right away.`
4. **Customer:** `john@example.com`
5. **Bot:** `Perfect! ✅ Here's your invite link to the premium channel: [link] Welcome aboard! 🎉`

**Feels like messaging support, not a robot.**

**If they cancel:**
- Gumroad sends webhook to your service
- Service looks up their Telegram ID by email
- Service removes them from channel
- Done ✅

---

## 🔧 Zero Manual Work Required

- ✅ Users onboard themselves (bot asks for email)
- ✅ Auto-linking (bot stores email ↔ Telegram ID)
- ✅ Auto-removal on cancellation (webhook triggers removal)
- ✅ Works 24/7 automatically

---

## 📊 View Linked Users

```bash
curl https://your-render-url.onrender.com/admin/users
```

---

## 🧪 Test It

1. Message your bot
2. Send `/start`
3. Enter a test email: `test@example.com`
4. Bot will link you and send invite link
5. Check linked users: `curl https://your-url.com/admin/users`

---

**That's it!** Fully automated Gumroad → Telegram subscription management. 🎯
