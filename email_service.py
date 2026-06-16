import os
import resend
import logging
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Initialize Resend with API key from environment
resend.api_key = os.getenv("RESEND_API_KEY", "")

class EmailService:
    """
    Email service for sending stock alerts using Resend API
    Free tier: 100 emails/day, 3,000 emails/month
    """
    
    FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev")
    
    @staticmethod
    def send_stock_alert(recipients: List[str], stock_symbol: str, price_data: Dict) -> Dict:
        """
        Send stock price alert to multiple recipients
        
        Args:
            recipients: List of email addresses
            stock_symbol: Stock symbol (e.g., 'OGDC')
            price_data: Dict with 'current_price', 'base_price', 'change_percent', 'direction'
        
        Returns:
            Dict with 'success' (bool), 'sent_count' (int), 'failed' (list), 'message' (str)
        """
        if not resend.api_key or resend.api_key == "":
            logger.error("Resend API key not configured")
            return {
                'success': False,
                'sent_count': 0,
                'failed': recipients,
                'message': 'Email service not configured. Please set RESEND_API_KEY.'
            }
        
        current_price = price_data.get('current_price', 0)
        base_price = price_data.get('base_price', 0)
        change_percent = price_data.get('change_percent', 0)
        direction = price_data.get('direction', 'up')
        volume = price_data.get('volume', 0)
        
        # Create email content
        arrow = "📈" if direction == 'up' else "📉"
        direction_text = "INCREASED" if direction == 'up' else "DECREASED"
        
        subject = f"🚨 PSX Alert: {stock_symbol} {direction_text} by {abs(change_percent):.2f}%"
        
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Arial', sans-serif; background-color: #f4f4f4; margin: 0; padding: 20px; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); overflow: hidden; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}
                .header h1 {{ margin: 0; font-size: 28px; }}
                .content {{ padding: 30px; }}
                .alert-box {{ background-color: {'#e8f5e9' if direction == 'up' else '#ffebee'}; border-left: 4px solid {'#4caf50' if direction == 'up' else '#f44336'}; padding: 20px; margin: 20px 0; border-radius: 5px; }}
                .stock-symbol {{ font-size: 32px; font-weight: bold; color: #333; margin-bottom: 10px; }}
                .price-info {{ display: flex; justify-content: space-between; margin: 15px 0; }}
                .price-item {{ text-align: center; }}
                .price-label {{ font-size: 12px; color: #666; text-transform: uppercase; }}
                .price-value {{ font-size: 24px; font-weight: bold; color: #333; }}
                .change {{ font-size: 36px; font-weight: bold; color: {'#4caf50' if direction == 'up' else '#f44336'}; text-align: center; margin: 20px 0; }}
                .footer {{ background-color: #f9f9f9; padding: 20px; text-align: center; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{arrow} Stock Price Alert</h1>
                </div>
                <div class="content">
                    <div class="alert-box">
                        <div class="stock-symbol">{stock_symbol}</div>
                        <div class="change">{arrow} {abs(change_percent):.2f}%</div>
                        <div class="price-info">
                            <div class="price-item">
                                <div class="price-label">Base Price</div>
                                <div class="price-value">Rs. {base_price:.2f}</div>
                            </div>
                            <div class="price-item">
                                <div class="price-label">Current Price</div>
                                <div class="price-value">Rs. {current_price:.2f}</div>
                            </div>
                        </div>
                        <div style="background: rgba(0,0,0,0.05); padding: 15px; border-radius: 8px; margin-top: 15px; text-align: center;">
                            <span style="color: #666; text-transform: uppercase; font-size: 12px; display: block;">Traded Volume</span>
                            <span style="font-size: 20px; font-weight: bold; color: #333;">{volume:,} shares</span>
                        </div>
                        <p style="text-align: center; margin-top: 20px; font-size: 16px;">
                            <strong>{stock_symbol}</strong> has <strong>{direction_text}</strong> by <strong>{abs(change_percent):.2f}%</strong>
                        </p>
                    </div>
                    <p style="color: #666; font-size: 14px;">
                        Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    </p>
                </div>
                <div class="footer">
                    <p>This is an automated alert from PSX Stock Tracker</p>
                    <p>© {datetime.now().year} PSX Stock Tracker. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        PSX STOCK ALERT
        
        {stock_symbol} has {direction_text} by {abs(change_percent):.2f}%
        
        Base Price: Rs. {base_price:.2f}
        Current Price: Rs. {current_price:.2f}
        Change: {arrow} {abs(change_percent):.2f}%
        Volume: {volume:,} shares
        
        Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        ---
        PSX Stock Tracker
        """
        
        # Send emails
        sent_count = 0
        failed = []
        
        for recipient_email in recipients:
            try:
                params = {
                    "from": EmailService.FROM_EMAIL,
                    "to": [recipient_email],
                    "subject": subject,
                    "html": html_body,
                    "text": text_body
                }
                
                resend.Emails.send(params)
                sent_count += 1
                logger.info(f"✅ Email sent to {recipient_email} for {stock_symbol}")
                
            except Exception as e:
                logger.error(f"❌ Failed to send email to {recipient_email}: {e}")
                failed.append(recipient_email)
        
        success = sent_count > 0
        message = f"Successfully sent {sent_count}/{len(recipients)} emails"
        
        if failed:
            message += f". Failed: {', '.join(failed)}"
        
        return {
            'success': success,
            'sent_count': sent_count,
            'failed': failed,
            'message': message
        }
    
    @staticmethod
    def send_custom_alert(recipients: List[str], subject: str, message: str, html_message: Optional[str] = None) -> Dict:
        """
        Send custom alert to multiple recipients
        
        Args:
            recipients: List of email addresses
            subject: Email subject
            message: Plain text message
            html_message: Optional HTML message
        
        Returns:
            Dict with 'success', 'sent_count', 'failed', 'message'
        """
        if not resend.api_key or resend.api_key == "":
            logger.error("Resend API key not configured")
            return {
                'success': False,
                'sent_count': 0,
                'failed': recipients,
                'message': 'Email service not configured'
            }
        
        sent_count = 0
        failed = []
        
        for recipient_email in recipients:
            try:
                params = {
                    "from": EmailService.FROM_EMAIL,
                    "to": [recipient_email],
                    "subject": subject,
                    "text": message
                }
                
                if html_message:
                    params["html"] = html_message
                
                resend.Emails.send(params)
                sent_count += 1
                logger.info(f"✅ Custom email sent to {recipient_email}")
                
            except Exception as e:
                logger.error(f"❌ Failed to send custom email to {recipient_email}: {e}")
                failed.append(recipient_email)
        
        return {
            'success': sent_count > 0,
            'sent_count': sent_count,
            'failed': failed,
            'message': f"Sent {sent_count}/{len(recipients)} emails"
        }
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Basic email validation"""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
