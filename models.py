from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

# ===== EXISTING MODEL (kept for compatibility) =====
class StockAlert(db.Model):
    """Individual stock alert for a single recipient"""
    id = db.Column(db.Integer, primary_key=True)
    email_address = db.Column(db.String(100), nullable=False)
    stock_symbol = db.Column(db.String(10), nullable=False)
    base_price = db.Column(db.Float, nullable=False)
    current_price = db.Column(db.Float)
    alert_threshold = db.Column(db.Float, default=5.0)
    alert_direction = db.Column(db.String(10), default='both')
    is_active = db.Column(db.Boolean, default=True)
    last_checked = db.Column(db.DateTime)
    base_price_updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    next_base_update = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def should_update_base_price(self):
        if not self.next_base_update:
            return True
        from datetime import timedelta
        return datetime.utcnow() >= self.next_base_update
    
    def update_base_price_schedule(self):
        from datetime import timedelta
        self.base_price_updated_at = datetime.utcnow()
        self.next_base_update = datetime.utcnow() + timedelta(seconds=1800)
        self.last_checked = datetime.utcnow()


# ===== NEW MODELS FOR MULTI-RECIPIENT ALERTS =====

class Company(db.Model):
    """Company or organization grouping for recipients"""
    __tablename__ = 'company'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    description = db.Column(db.Text)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    recipients = db.relationship('Recipient', backref='company', lazy=True, cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Company {self.name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'is_active': self.is_active,
            'recipient_count': len([r for r in self.recipients if r.is_active]),
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


class Recipient(db.Model):
    """Email recipient for stock alerts"""
    __tablename__ = 'recipient'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(120), nullable=False, unique=True)
    company_id = db.Column(db.Integer, db.ForeignKey('company.id'), nullable=True)
    phone = db.Column(db.String(20))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    alert_logs = db.relationship('AlertLog', backref='recipient', lazy=True)
    
    def __repr__(self):
        return f'<Recipient {self.name} ({self.email})>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'email': self.email,
            'company_id': self.company_id,
            'company_name': self.company.name if self.company else None,
            'phone': self.phone,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


class AlertTemplate(db.Model):
    """Reusable email templates for alerts"""
    __tablename__ = 'alert_template'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    subject_template = db.Column(db.String(200), nullable=False)
    body_template = db.Column(db.Text, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<AlertTemplate {self.name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'subject_template': self.subject_template,
            'body_template': self.body_template,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


class AlertLog(db.Model):
    """Log of sent alerts for tracking"""
    __tablename__ = 'alert_log'
    
    id = db.Column(db.Integer, primary_key=True)
    recipient_id = db.Column(db.Integer, db.ForeignKey('recipient.id'), nullable=False)
    stock_symbol = db.Column(db.String(10), nullable=False)
    alert_type = db.Column(db.String(50))  # 'price_change', 'custom', etc.
    subject = db.Column(db.String(200))
    body = db.Column(db.Text)
    status = db.Column(db.String(20), default='sent')  # 'sent', 'failed', 'pending'
    error_message = db.Column(db.Text)
    sent_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<AlertLog {self.stock_symbol} to {self.recipient_id}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'recipient_id': self.recipient_id,
            'recipient_name': self.recipient.name if self.recipient else None,
            'stock_symbol': self.stock_symbol,
            'alert_type': self.alert_type,
            'subject': self.subject,
            'status': self.status,
            'error_message': self.error_message,
            'sent_at': self.sent_at.isoformat() if self.sent_at else None
        }
