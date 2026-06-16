# Email Configuration for EmailJS
# Get your credentials from https://emailjs.com

class EmailConfig:
    """
    EmailJS Configuration
    
    To set up:
    1. Create account at https://emailjs.com
    2. Create an email service (Gmail, etc.)
    3. Create an email template with these variables:
       - {{to_email}} - recipient email
       - {{symbol}} - stock symbol
       - {{current_price}} - current price
       - {{base_price}} - base price
       - {{change_percent}} - percentage change
       - {{threshold}} - alert threshold
       - {{direction}} - up/down
       - {{timestamp}} - alert time
    4. Copy your credentials here
    """
    
    # REPLACE THESE WITH YOUR EMAILJS CREDENTIALS
    SERVICE_ID = "your_service_id"
    TEMPLATE_ID = "your_template_id"
    PUBLIC_KEY = "your_public_key"
    
    # Email template for alerts
    @staticmethod
    def create_alert_email_params(email, symbol, current_price, base_price, change_percent, threshold, direction, volume=0):
        """Create email parameters for EmailJS"""
        from datetime import datetime
        
        arrow = "📈" if direction == 'up' else "📉"
        direction_text = "INCREASED" if direction == 'up' else "DECREASED"
        
        return {
            'to_email': email,
            'symbol': symbol,
            'current_price': f"Rs. {current_price:.2f}",
            'base_price': f"Rs. {base_price:.2f}",
            'change_percent': f"{abs(change_percent):.2f}%",
            'threshold': f"{threshold}%",
            'direction': direction_text,
            'volume': f"{volume:,}",
            'arrow': arrow,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'subject': f"🚨 PSX Alert: {symbol} {direction_text} by {abs(change_percent):.2f}%"
        }
