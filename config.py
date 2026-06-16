import os
from datetime import timedelta

class Config:
    # Flask
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")
    
    # Database
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///alerts.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # PSX API
    PSX_API_BASE_URL = "https://psxterminal.com"
    PSX_API_TIMEOUT = 10
    
    # WhatsApp (Twilio)
    TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
    TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
    TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")
    
    # Monitoring — all from environment variables (see .env.example)
    ALERT_THRESHOLD       = float(os.getenv("ALERT_THRESHOLD",       "4.0"))     # % surge
    MIN_VALUE_THRESHOLD   = float(os.getenv("MIN_VALUE_THRESHOLD",   "20000000")) # Rs.
    MONITOR_WINDOW        = int(os.getenv("MONITOR_WINDOW",         "1200"))     # seconds
    PRICE_CHECK_INTERVAL  = int(os.getenv("PRICE_CHECK_INTERVAL",   "60"))       # seconds
    BASE_PRICE_UPDATE_INTERVAL = int(os.getenv("BASE_PRICE_UPDATE_INTERVAL", "1800"))  # seconds
    
    # Rate limiting — from environment
    MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "90"))
    REQUEST_DELAY           = float(os.getenv("REQUEST_DELAY",         "0.65"))  # seconds