from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit
from datetime import datetime, timedelta
import time
import threading
import os
import logging
import requests
import sys
import json

# ── Alert logic — pulled from environment vars (same as monitor.py / .env.example) ──
_ALERT_THRESHOLD    = float(os.environ.get("ALERT_THRESHOLD",    "4.0"))     # % surge
_MIN_VALUE_THRESHOLD = float(os.environ.get("MIN_VALUE_THRESHOLD", "20000000")) # Rs. min traded value
_MONITOR_WINDOW     = int(os.environ.get("MONITOR_WINDOW",     "1200"))     # seconds

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY", "psx-stock-tracker-secret")
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DATABASE_URL", "sqlite:///alerts.db")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ===== DATABASE MODELS =====
from models import db, StockAlert, Company, Recipient, AlertTemplate, AlertLog
db.init_app(app)

# ===== PSX WEBSOCKET MONITOR =====
from websocket_monitor import PSXWebSocketMonitor

# Initialize WebSocket monitor
ws_monitor = PSXWebSocketMonitor()
logger.info("WebSocket monitor initialized")

# ===== EMAIL CONFIG =====
from email_config import EmailConfig
from email_service import EmailService

# ===== FLASK ROUTES =====
@app.route('/')
def index():
    """Main dashboard - shows alerts"""
    alerts = StockAlert.query.filter_by(is_active=True).order_by(StockAlert.created_at.desc()).all()
    return render_template('index.html', alerts=alerts)

@app.route('/dashboard')
def dashboard():
    """Live dashboard showing ALL companies"""
    return render_template('dashboard.html')

@app.route('/api/dashboard-data')
def get_dashboard_data():
    """Get dashboard data for AJAX"""
    prices = ws_monitor.get_all_prices()
    
    # Calculate statistics
    gainers = losers = unchanged = 0
    updated = 0
    
    for symbol, data in prices.items():
        if 'price' in data and data['price'] is not None:
            updated += 1
        
        if 'change_percent' in data and data['change_percent'] is not None:
            change = data['change_percent']
            if change > 0:
                gainers += 1
            elif change < 0:
                losers += 1
            else:
                unchanged += 1
    
    return jsonify({
        'prices': prices,
        'last_update': time.time(),
        'stats': {
            'total_symbols': len(prices),
            'updated_symbols': updated,
            'gainers': gainers,
            'losers': losers,
            'unchanged': unchanged
        }
    })

@app.route('/api/top-gainers')
def top_gainers():
    """Get top 20 gainers"""
    prices = ws_monitor.get_all_prices()
    
    gainers = []
    for symbol, data in prices.items():
        if 'change_percent' in data and data.get('change_percent', 0) > 0:
            gainers.append({
                'symbol': symbol,
                'price': data.get('price', 0),
                'change_percent': data.get('change_percent', 0),
                'change': data.get('change', 0)
            })
    
    gainers.sort(key=lambda x: x['change_percent'], reverse=True)
    return jsonify(gainers[:20])

@app.route('/api/top-losers')
def top_losers():
    """Get top 20 losers"""
    prices = ws_monitor.get_all_prices()
    
    losers = []
    for symbol, data in prices.items():
        if 'change_percent' in data and data.get('change_percent', 0) < 0:
            losers.append({
                'symbol': symbol,
                'price': data.get('price', 0),
                'change_percent': data.get('change_percent', 0),
                'change': data.get('change', 0)
            })
    
    losers.sort(key=lambda x: x['change_percent'])
    return jsonify(losers[:20])

@app.route('/api/search-symbol/<symbol>')
def search_symbol(symbol):
    """Search for specific symbol"""
    price_data = ws_monitor.get_price(symbol.upper())
    if price_data and price_data.get('price'):
        return jsonify(price_data)
    return jsonify({'error': 'Symbol not found'}), 404

@app.route('/api/all-symbols')
def get_all_symbols():
    """Get all tracked symbols"""
    prices = ws_monitor.get_all_prices()
    symbols = list(prices.keys())
    return jsonify({'symbols': symbols, 'count': len(symbols)})

@app.route('/create-alert', methods=['POST'])
def create_alert():
    """Create new stock alert"""
    try:
        email = request.form.get('email')
        
        # Validate email
        if not email or '@' not in email:
            return jsonify({'error': 'Invalid email address'}), 400
        
        symbol = request.form.get('symbol', '').strip().upper()
        if not symbol:
            return jsonify({'error': 'Symbol is required'}), 400
        threshold = float(request.form.get('threshold', 5.0))
        direction = request.form.get('direction', 'both')
        
        # Get current price from WebSocket cache or REST API fallback
        price_data = ws_monitor.get_price(symbol)
        if not price_data or price_data.get('price') is None:
            logger.error(f"❌ Failed to fetch price for {symbol}. Price data: {price_data}")
            return jsonify({'error': f'Could not fetch current price for {symbol}. Please verify symbol and try again.'}), 400
        
        current_price = price_data['price']
        
        # Save to database
        alert = StockAlert(
            email_address=email,
            stock_symbol=symbol,
            base_price=current_price,
            current_price=current_price,
            alert_threshold=threshold,
            alert_direction=direction,
            is_active=True
        )
        alert.update_base_price_schedule()
        
        db.session.add(alert)
        db.session.commit()
        
        logger.info(f"✅ Alert created: {symbol} for {email}")
        
        return redirect(url_for('index'))
        
    except Exception as e:
        logger.error(f"❌ Error creating alert: {e}")
        return jsonify({'error': str(e)}), 400

@app.route('/deactivate-alert/<int:alert_id>')
def deactivate_alert(alert_id):
    """Deactivate an alert"""
    try:
        alert = StockAlert.query.get(alert_id)
        if alert:
            alert.is_active = False
            db.session.commit()
            logger.info(f"✅ Alert {alert_id} deactivated")
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f"❌ Error deactivating alert: {e}")
        return redirect(url_for('index'))

@app.route('/health')
def health_check():
    """Health check endpoint"""
    stats = ws_monitor.get_stats()
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'websocket_connected': stats.get('connected', False),
        'cached_symbols': stats.get('cached_symbols', 0),
        'active_alerts': StockAlert.query.filter_by(is_active=True).count()
    })

# ===== ADMIN PORTAL ROUTES =====

@app.route('/admin')
def admin_dashboard():
    """Admin dashboard homepage"""
    stats = {
        'total_recipients': Recipient.query.filter_by(is_active=True).count(),
        'total_companies': Company.query.filter_by(is_active=True).count(),
        'total_alerts_sent': AlertLog.query.filter_by(status='sent').count(),
        'recent_alerts': AlertLog.query.order_by(AlertLog.sent_at.desc()).limit(10).all()
    }
    return render_template('admin.html', stats=stats)

@app.route('/admin/recipients')
def admin_recipients():
    """Recipient management page"""
    recipients = Recipient.query.filter_by(is_active=True).all()
    companies = Company.query.filter_by(is_active=True).all()
    return render_template('recipients.html', recipients=recipients, companies=companies)

@app.route('/admin/companies')
def admin_companies():
    """Company management page"""
    companies = Company.query.filter_by(is_active=True).all()
    return render_template('companies.html', companies=companies)

@app.route('/admin/bulk-alerts')
def admin_bulk_alerts():
    """Bulk alert creation page"""
    recipients = Recipient.query.filter_by(is_active=True).all()
    companies = Company.query.filter_by(is_active=True).all()
    return render_template('bulk_alerts.html', recipients=recipients, companies=companies)

# ===== ADMIN API ROUTES =====

# Recipient API
@app.route('/api/recipients', methods=['GET'])
def get_recipients():
    """Get all active recipients"""
    recipients = Recipient.query.filter_by(is_active=True).all()
    return jsonify([r.to_dict() for r in recipients])

@app.route('/api/recipients/<int:recipient_id>', methods=['GET'])
def get_recipient(recipient_id):
    """Get a specific recipient"""
    recipient = Recipient.query.get_or_404(recipient_id)
    return jsonify(recipient.to_dict())

@app.route('/api/recipients', methods=['POST'])
def create_recipient():
    """Create a new recipient"""
    try:
        data = request.get_json()
        
        # Validate email
        email = data.get('email', '').strip()
        if not EmailService.validate_email(email):
            return jsonify({'error': 'Invalid email address'}), 400
        
        # Check for duplicate email
        existing = Recipient.query.filter_by(email=email).first()
        if existing:
            return jsonify({'error': 'Email already exists'}), 400
        
        recipient = Recipient(
            name=data.get('name', '').strip(),
            email=email,
            company_id=data.get('company_id'),
            phone=data.get('phone', '').strip(),
            is_active=True
        )
        
        db.session.add(recipient)
        db.session.commit()
        
        logger.info(f"✅ Created recipient: {recipient.name} ({recipient.email})")
        return jsonify(recipient.to_dict()), 201
        
    except Exception as e:
        logger.error(f"❌ Error creating recipient: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 400

@app.route('/api/recipients/<int:recipient_id>', methods=['PUT'])
def update_recipient(recipient_id):
    """Update a recipient"""
    try:
        recipient = Recipient.query.get_or_404(recipient_id)
        data = request.get_json()
        
        # Validate email if changed
        new_email = data.get('email', recipient.email).strip()
        if new_email != recipient.email:
            if not EmailService.validate_email(new_email):
                return jsonify({'error': 'Invalid email address'}), 400
            
            # Check for duplicate
            existing = Recipient.query.filter_by(email=new_email).first()
            if existing and existing.id != recipient_id:
                return jsonify({'error': 'Email already exists'}), 400
        
        recipient.name = data.get('name', recipient.name)
        recipient.email = new_email
        recipient.company_id = data.get('company_id', recipient.company_id)
        recipient.phone = data.get('phone', recipient.phone)
        
        db.session.commit()
        
        logger.info(f"✅ Updated recipient: {recipient.name}")
        return jsonify(recipient.to_dict())
        
    except Exception as e:
        logger.error(f"❌ Error updating recipient: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 400

@app.route('/api/recipients/<int:recipient_id>', methods=['DELETE'])
def delete_recipient(recipient_id):
    """Delete (deactivate) a recipient"""
    try:
        recipient = Recipient.query.get_or_404(recipient_id)
        recipient.is_active = False
        db.session.commit()
        
        logger.info(f"✅ Deleted recipient: {recipient.name}")
        return jsonify({'success': True, 'message': 'Recipient deleted'})
        
    except Exception as e:
        logger.error(f"❌ Error deleting recipient: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 400

# Company API
@app.route('/api/companies', methods=['GET'])
def get_companies():
    """Get all active companies"""
    companies = Company.query.filter_by(is_active=True).all()
    return jsonify([c.to_dict() for c in companies])

@app.route('/api/companies', methods=['POST'])
def create_company():
    """Create a new company"""
    try:
        data = request.get_json()
        
        name = data.get('name', '').strip()
        if not name:
            return jsonify({'error': 'Company name is required'}), 400
        
        # Check for duplicate
        existing = Company.query.filter_by(name=name).first()
        if existing:
            return jsonify({'error': 'Company already exists'}), 400
        
        company = Company(
            name=name,
            description=data.get('description', '').strip(),
            is_active=True
        )
        
        db.session.add(company)
        db.session.commit()
        
        logger.info(f"✅ Created company: {company.name}")
        return jsonify(company.to_dict()), 201
        
    except Exception as e:
        logger.error(f"❌ Error creating company: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 400

@app.route('/api/companies/<int:company_id>', methods=['PUT'])
def update_company(company_id):
    """Update a company"""
    try:
        company = Company.query.get_or_404(company_id)
        data = request.get_json()
        
        company.name = data.get('name', company.name)
        company.description = data.get('description', company.description)
        
        db.session.commit()
        
        logger.info(f"✅ Updated company: {company.name}")
        return jsonify(company.to_dict())
        
    except Exception as e:
        logger.error(f"❌ Error updating company: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 400

@app.route('/api/companies/<int:company_id>', methods=['DELETE'])
def delete_company(company_id):
    """Delete (deactivate) a company"""
    try:
        company = Company.query.get_or_404(company_id)
        company.is_active = False
        db.session.commit()
        
        logger.info(f"✅ Deleted company: {company.name}")
        return jsonify({'success': True, 'message': 'Company deleted'})
        
    except Exception as e:
        logger.error(f"❌ Error deleting company: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 400

@app.route('/api/recipients/by-company/<int:company_id>', methods=['GET'])
def get_recipients_by_company(company_id):
    """Get all recipients for a specific company"""
    recipients = Recipient.query.filter_by(company_id=company_id, is_active=True).all()
    return jsonify([r.to_dict() for r in recipients])

# Bulk Alert API
@app.route('/api/bulk-alert', methods=['POST'])
def send_bulk_alert():
    """Send stock alert to multiple recipients"""
    try:
        data = request.get_json()
        
        stock_symbols = data.get('stock_symbols', [])
        recipient_ids = data.get('recipient_ids', [])
        company_ids = data.get('company_ids', [])
        
        if not stock_symbols:
            return jsonify({'error': 'At least one stock symbol required'}), 400
        
        # Collect recipient emails
        recipient_emails = set()
        
        # Add direct recipients
        if recipient_ids:
            recipients = Recipient.query.filter(
                Recipient.id.in_(recipient_ids),
                Recipient.is_active == True
            ).all()
            for r in recipients:
                recipient_emails.add((r.id, r.email))
        
        # Add company recipients
        if company_ids:
            company_recipients = Recipient.query.filter(
                Recipient.company_id.in_(company_ids),
                Recipient.is_active == True
            ).all()
            for r in company_recipients:
                recipient_emails.add((r.id, r.email))
        
        if not recipient_emails:
            return jsonify({'error': 'No valid recipients selected'}), 400
        
        # Send alerts for each stock
        results = []
        for symbol in stock_symbols:
            symbol = symbol.upper().strip()
            
            # Get current price
            price_data_raw = ws_monitor.get_price(symbol)
            if not price_data_raw or price_data_raw.get('price') is None:
                results.append({
                    'symbol': symbol,
                    'success': False,
                    'error': 'Could not fetch price data'
                })
                continue
            
            current_price = price_data_raw['price']
            change_percent = price_data_raw.get('change_percent', 0)
            
            # Determine direction
            direction = 'up' if change_percent >= 0 else 'down'
            
            # Prepare price data for email
            price_data = {
                'current_price': current_price,
                'base_price': price_data_raw.get('ldcp', current_price),
                'change_percent': change_percent,
                'direction': direction,
                'volume': price_data_raw.get('volume', 0)
            }
            
            # Send emails
            emails_only = [email for _, email in recipient_emails]
            email_result = EmailService.send_stock_alert(emails_only, symbol, price_data)
            
            # Log each alert
            for recipient_id, email in recipient_emails:
                status = 'sent' if email not in email_result.get('failed', []) else 'failed'
                
                alert_log = AlertLog(
                    recipient_id=recipient_id,
                    stock_symbol=symbol,
                    alert_type='bulk_price_alert',
                    subject=f"PSX Alert: {symbol}",
                    body=f"Price: Rs. {current_price:.2f}, Change: {change_percent:.2f}%",
                    status=status,
                    error_message=None if status == 'sent' else 'Failed to send'
                )
                db.session.add(alert_log)
            
            results.append({
                'symbol': symbol,
                'success': email_result['success'],
                'sent_count': email_result['sent_count'],
                'message': email_result['message']
            })
        
        db.session.commit()
        
        total_sent = sum(r.get('sent_count', 0) for r in results)
        logger.info(f"✅ Bulk alert sent: {total_sent} emails for {len(stock_symbols)} stocks")
        
        return jsonify({
            'success': True,
            'results': results,
            'total_sent': total_sent
        })
        
    except Exception as e:
        logger.error(f"❌ Error sending bulk alert: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 400

# ===== SOCKETIO EVENTS =====
@socketio.on('connect')
def handle_connect():
    """Client connected to SocketIO"""
    logger.info("Client connected to SocketIO")
    emit('connection_status', {'status': 'connected', 'timestamp': datetime.now().isoformat()})

@socketio.on('disconnect')
def handle_disconnect():
    """Client disconnected from SocketIO"""
    logger.info("Client disconnected from SocketIO")

@socketio.on('request_prices')
def handle_request_prices():
    """Client requested current prices"""
    prices = ws_monitor.get_all_prices()
    emit('price_update', prices)

# ===== BACKGROUND TASKS =====
def broadcast_price_updates():
    """Broadcast price updates to all connected clients"""
    with app.app_context():
        while True:
            try:
                time.sleep(2)  # Broadcast every 2 seconds
                prices = ws_monitor.get_all_prices()
                
                # Broadcast to all connected clients
                socketio.emit('price_update', prices, namespace='/')
                
            except Exception as e:
                logger.error(f"Broadcast error: {e}")
                time.sleep(5)

def check_alerts():
    """Check alerts against WebSocket prices"""
    with app.app_context():
        time.sleep(10)  # Initial delay
        
        while True:
            try:
                alerts = StockAlert.query.filter_by(is_active=True).all()
                
                if alerts:
                    logger.info(f"🔔 Checking {len(alerts)} active alerts")
                    
                    for alert in alerts:
                        price_data = ws_monitor.get_price(alert.stock_symbol)
                        
                        if price_data and price_data.get('price'):
                            current_price = price_data['price']
                            alert.current_price = current_price
                            alert.last_checked = datetime.utcnow()
                            
                            # Calculate change
                            # App logic uses whole numbers for thresholds (e.g. 5.0 for 5%)
                            if current_price:
                                # Update price history for this alert (rolling window from env)
                                if not hasattr(alert, 'price_history'):
                                    alert.price_history = []
                                
                                now = datetime.utcnow()
                                alert.price_history.append((now, current_price))
                                # Keep only the rolling window defined in MONITOR_WINDOW
                                alert.price_history = [p for p in alert.price_history if (now - p[0]).total_seconds() <= _MONITOR_WINDOW]
                                
                                if len(alert.price_history) < 2:
                                    continue
                                
                                # Check surge logic using env-configured threshold
                                old_price = alert.price_history[0][1]
                                change_percent = ((current_price - old_price) / old_price) * 100
                                
                                # Check Value condition (from env)
                                volume = price_data.get('volume', 0)
                                traded_value = current_price * volume
                                
                                if change_percent >= _ALERT_THRESHOLD and traded_value >= _MIN_VALUE_THRESHOLD:
                                    direction = 'up'
                                    logger.info(f"🚨 ALERT: {alert.stock_symbol} surged {change_percent:+.2f}% in {_MONITOR_WINDOW//60} mins (Value: Rs. {traded_value:,.0f})")
                                    
                                    # Prepare email data for frontend
                                    email_params = EmailConfig.create_alert_email_params(
                                        alert.email_address,
                                        alert.stock_symbol,
                                        current_price,
                                        old_price,
                                        change_percent,
                                        _ALERT_THRESHOLD,
                                        direction,
                                        volume
                                    )
                                    
                                    # Broadcast alert to frontend (EmailJS will send from browser)
                                    socketio.emit('trigger_email_alert', email_params, namespace='/')
                                    
                                    # Deactivate
                                    alert.is_active = False
                            
                            # Update base price every 30 minutes
                            if alert.should_update_base_price():
                                alert.base_price = current_price
                                alert.update_base_price_schedule()
                            
                            db.session.commit()
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"❌ Alert check error: {e}")
                time.sleep(60)

# ===== START APP =====
def print_startup_banner():
    """Print fancy startup banner"""
    print("\n" + "="*70)
    print("PSX STOCK TRACKER - LIVE WEBSOCKET MONITOR".center(70))
    print("="*70)
    print(f"Server Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {app.config['SQLALCHEMY_DATABASE_URI']}")
    print("="*70)
    print("Database initialized")
    print("WebSocket monitor started")
    print("SocketIO server ready")
    print("Email alerts configured (EmailJS)")
    print("="*70)
    print("DASHBOARD: http://localhost:5000/dashboard".center(70))
    print("HOME: http://localhost:5000".center(70))
    print("HEALTH CHECK: http://localhost:5000/health".center(70))
    print("="*70)
    print("FEATURES:".center(70))
    print("Real-time PSX WebSocket monitoring".center(70))
    print("Live dashboard for ALL companies".center(70))
    print("Email alerts via EmailJS".center(70))
    print("Instant price updates via SocketIO".center(70))
    print("="*70 + "\n")

if __name__ == '__main__':
    # Force flush output
    sys.stdout.flush()
    
    # Create database
    with app.app_context():
        db.create_all()
        logger.info("Database initialized")
    
    # Start background threads
    broadcast_thread = threading.Thread(target=broadcast_price_updates, daemon=True)
    broadcast_thread.start()
    logger.info("Price broadcast thread started")
    
    alert_thread = threading.Thread(target=check_alerts, daemon=True)
    alert_thread.start()
    logger.info("Alert checker thread started")
    
    # Print banner
    time.sleep(2)
    print_startup_banner()
    
    # Run SocketIO app
    try:
        socketio.run(app, debug=True, host='0.0.0.0', port=5000, use_reloader=False, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        print("\nServer shutting down...")
        sys.exit(0)