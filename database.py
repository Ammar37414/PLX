from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta

db = SQLAlchemy()

class StockAlert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    phone_number = db.Column(db.String(20), nullable=False)
    stock_symbol = db.Column(db.String(10), nullable=False)
    base_price = db.Column(db.Float, nullable=False)
    current_price = db.Column(db.Float)
    alert_threshold = db.Column(db.Float, default=5.0)
    alert_direction = db.Column(db.String(10), default='both')  # up, down, both
    is_active = db.Column(db.Boolean, default=True)
    last_checked = db.Column(db.DateTime)
    base_price_updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    next_base_update = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<Alert {self.stock_symbol} for {self.phone_number}>'
    
    def should_update_base_price(self):
        if not self.next_base_update:
            return True
        return datetime.utcnow() >= self.next_base_update
    
    def update_base_price_schedule(self):
        self.base_price_updated_at = datetime.utcnow()
        self.next_base_update = datetime.utcnow() + timedelta(seconds=1800)
        self.last_checked = datetime.utcnow()

class AlertHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    alert_id = db.Column(db.Integer, db.ForeignKey('stock_alert.id'), nullable=False)
    trigger_price = db.Column(db.Float)
    trigger_percentage = db.Column(db.Float)
    direction = db.Column(db.String(10))  # up, down
    sent_at = db.Column(db.DateTime, default=datetime.utcnow)
    notification_status = db.Column(db.String(20), default='sent')
    
    alert = db.relationship('StockAlert', backref=db.backref('history', lazy=True))