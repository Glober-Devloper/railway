# FileSaver Bot (Supabase + Railway Ready)

This bot stores Telegram files in a private channel and manages links with Supabase (PostgreSQL).

## 🚀 Deployment on Railway
1. Push this project to GitHub.
2. Create a new Railway project -> Deploy from GitHub.
3. Add required environment variables:
   - BOT_TOKEN
   - API_ID
   - API_HASH
   - STORAGE_CHANNEL_ID
   - BOT_USERNAME
   - ADMIN_IDS
   - ADMIN_CONTACT
   - SUPABASE_URL
4. Railway will auto-deploy your bot.

Procfile ensures worker mode, not web mode.
