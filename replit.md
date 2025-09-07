# Stock Market Scanner

A real-time stock market scanner that monitors market data and provides trading alerts via Telegram and Discord.

## Overview

This Python application connects to live market data feeds and performs technical analysis to identify trading opportunities. It automatically detects various market events and sends alerts when specific criteria are met.

## Key Features

- **Real-time Data**: Connects to Polygon.io WebSocket for live market data
- **Technical Analysis**: Calculates EMA, RSI, VWAP, Bollinger Bands, MACD
- **Alert System**: Sends notifications via Telegram and Discord
- **News Monitoring**: Scrapes Yahoo Finance and monitors NASDAQ halts
- **Machine Learning**: Uses trained models to score trading events
- **Volume Analysis**: Tracks volume spikes and relative volume (RVOL)

## Current Setup

- **Language**: Python 3.11
- **Main Application**: scanner.py 
- **Dependencies**: All installed from requirements.txt
- **Workflow**: Runs automatically as "Stock Scanner" background task
- **Market Hours**: Active 4am-8pm EST, Monday-Friday

## Project Structure

- `scanner.py` - Main application with all trading logic
- `runner_model.joblib` - Pre-trained ML model for event scoring
- `requirements.txt` - Python dependencies
- `*.csv` files - Historical trading data and event logs
- Training scripts for ML model development

## Current Status

✅ Application running successfully  
✅ All dependencies installed  
✅ ML model loaded  
✅ Monitoring market schedule (currently paused - market closed)

## Configuration

The application uses environment variables for API keys:
- `POLYGON_API_KEY` - Market data API key
- `TELEGRAM_BOT_TOKEN` - Telegram bot token
- `TELEGRAM_CHAT_ID` - Telegram chat ID

Default values are provided in the code for immediate functionality.

## Recent Changes

- Set up Python 3.11 environment in Replit
- Installed all required packages
- Configured workflow to run continuously
- Verified successful startup and market schedule detection
