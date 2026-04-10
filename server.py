#!/usr/bin/env python3
"""TULPAR — Social Commerce Platform Server"""

import csv
import hashlib
import io
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from functools import wraps

import requests as http_requests
from dotenv import load_dotenv
from flask import Flask, Response, g, jsonify, request, send_from_directory
from flask_cors import CORS

# ─── CONFIG ──────────────────────────────────────────────────────────────

BASE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE, '.env'))

PORT = int(os.environ.get('PORT', 3000))
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'tulpar2024')
JWT_SECRET = os.environ.get('JWT_SECRET', 'tulpar_secret_change_me')
TG_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TG_CHAT = os.environ.get('TELEGRAM_ADMIN_CHAT_ID', '')

DB_PATH = os.path.join(BASE, 'tulpar.db')

TARIFF_NAMES = {
    'start': 'СТАРТ — 15,000 ₸',
    'partner': 'ПАРТНЁР — 50,000 ₸',
    'leader': 'ЛИДЕР — 150,000 ₸',
}
TARIFF_PRICES = {'start': 15_000, 'partner': 50_000, 'leader': 150_000}
TARIFF_ORDER_COMM = {'start': 0.08, 'partner': 0.12, 'leader': 0.15}
REF_BONUS_L1 = 0.03  # 3% from package purchase
REF_BONUS_L2 = 0.01  # 1% from package purchase

app = Flask(__name__, static_folder='public', static_url_path='')
CORS(app)


# ─── DATABASE ────────────────────────────────────────────────────────────

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute('PRAGMA journal_mode=WAL')
    return g.db


@app.teardown_appcontext
def close_db(exc=None):
    db = g.pop('db', None)
    if db:
        db.close()


def hash_password(password):
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f'{salt}:{h}'


def check_password(stored, password):
    if not stored or ':' not in stored:
        return False
    salt, h = stored.split(':', 1)
    return hashlib.sha256((salt + password).encode()).hexdigest() == h


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS partners (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT    NOT NULL,
                phone       TEXT    NOT NULL UNIQUE,
                password    TEXT    DEFAULT '',
                email       TEXT    DEFAULT '',
                city        TEXT    DEFAULT '',
                tariff      TEXT    NOT NULL,
                ref_code    TEXT    UNIQUE,
                referrer    TEXT    DEFAULT '',
                status      TEXT    DEFAULT 'new',
                notes       TEXT    DEFAULT '',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS tokens (
                token       TEXT PRIMARY KEY,
                partner_id  INTEGER NOT NULL,
                is_admin    INTEGER DEFAULT 0,
                expires_at  DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS categories (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT    NOT NULL,
                icon        TEXT    DEFAULT '',
                sort_order  INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS products (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER,
                name        TEXT    NOT NULL,
                description TEXT    DEFAULT '',
                price       INTEGER NOT NULL,
                image_url   TEXT    DEFAULT '',
                in_stock    INTEGER DEFAULT 1,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS orders (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_ref   TEXT    DEFAULT '',
                customer_name TEXT    NOT NULL,
                customer_phone TEXT   NOT NULL,
                status        TEXT    DEFAULT 'new',
                total         INTEGER DEFAULT 0,
                created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS order_items (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id    INTEGER NOT NULL,
                product_id  INTEGER NOT NULL,
                quantity    INTEGER DEFAULT 1,
                price       INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_id          INTEGER NOT NULL,
                type                TEXT    NOT NULL,
                amount              INTEGER NOT NULL,
                description         TEXT    DEFAULT '',
                related_partner_id  INTEGER DEFAULT 0,
                created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Migrate existing tables
        for col, default in [('password', '""'), ('balance', '0')]:
            try:
                conn.execute(f'ALTER TABLE partners ADD COLUMN {col} TEXT DEFAULT {default}' if col == 'password' else f'ALTER TABLE partners ADD COLUMN {col} INTEGER DEFAULT {default}')
            except sqlite3.OperationalError:
                pass
        # Seed categories if empty
        if conn.execute('SELECT COUNT(*) FROM categories').fetchone()[0] == 0:
            conn.executemany(
                'INSERT INTO categories (name, icon, sort_order) VALUES (?, ?, ?)',
                [('Здоровье', '💊', 1), ('Красота', '💄', 2), ('Дом', '🏠', 3), ('Мода', '👗', 4)],
            )


init_db()


# ─── AUTH ────────────────────────────────────────────────────────────────

def _utcnow():
    return datetime.now(timezone.utc)


def create_token(partner_id=0, is_admin=False):
    token = secrets.token_hex(32)
    expires = _utcnow() + timedelta(days=1 if is_admin else 7)
    db = get_db()
    db.execute(
        'INSERT INTO tokens (token, partner_id, is_admin, expires_at) VALUES (?, ?, ?, ?)',
        (token, partner_id, 1 if is_admin else 0, expires.isoformat()),
    )
    db.commit()
    return token


def verify_token(token):
    if not token:
        return None
    db = get_db()
    row = db.execute(
        'SELECT * FROM tokens WHERE token = ? AND expires_at > ?',
        (token, _utcnow().isoformat()),
    ).fetchone()
    return dict(row) if row else None


def _get_bearer_token():
    header = request.headers.get('Authorization', '')
    if header.startswith('Bearer '):
        return header[7:].strip()
    return ''


def auth_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        payload = verify_token(_get_bearer_token())
        if not payload:
            return jsonify({'error': 'Нет авторизации'}), 401
        request.partner_id = payload['partner_id']
        request.is_admin = bool(payload['is_admin'])
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        payload = verify_token(_get_bearer_token())
        if not payload or not payload['is_admin']:
            return jsonify({'error': 'Нет доступа'}), 403
        request.is_admin = True
        return f(*args, **kwargs)
    return wrapper


# ─── TELEGRAM ────────────────────────────────────────────────────────────

def send_telegram(text):
    if not TG_TOKEN or TG_TOKEN == 'your_telegram_bot_token_here':
        return
    try:
        http_requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            json={'chat_id': TG_CHAT, 'text': text, 'parse_mode': 'HTML'},
            timeout=5,
        )
    except Exception as e:
        print(f'Telegram error: {e}')


# ─── STATIC PAGES ───────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/register')
def register_page():
    return send_from_directory(app.static_folder, 'register.html')


@app.route('/login')
def login_page():
    return send_from_directory(app.static_folder, 'login.html')


@app.route('/dashboard')
def dashboard_page():
    return send_from_directory(app.static_folder, 'dashboard.html')


@app.route('/admin')
def admin_page():
    return send_from_directory(app.static_folder, 'admin.html')


@app.route('/catalog')
def catalog_page():
    return send_from_directory(app.static_folder, 'catalog.html')


# ─── API: REGISTER ──────────────────────────────────────────────────────

@app.post('/api/register')
def api_register():
    d = request.get_json(force=True) or {}

    name = (d.get('name') or '').strip()
    phone = ''.join(c for c in (d.get('phone') or '') if c.isdigit())
    password = (d.get('password') or '').strip()
    email = (d.get('email') or '').strip()
    city = (d.get('city') or '').strip()
    tariff = 'free'  # always free at registration
    referrer = (d.get('referrer') or '').strip().upper()

    if not name or not phone or not password:
        return jsonify({'error': 'Заполните обязательные поля: имя, телефон, пароль'}), 400

    if len(password) < 4:
        return jsonify({'error': 'Пароль должен быть минимум 4 символа'}), 400

    db = get_db()
    if db.execute('SELECT id FROM partners WHERE phone = ?', (phone,)).fetchone():
        return jsonify({'error': 'Партнёр с таким номером уже зарегистрирован'}), 409

    ref_code = 'TLP' + secrets.token_hex(3).upper()

    db.execute(
        'INSERT INTO partners (name, phone, password, email, city, tariff, ref_code, referrer) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (name, phone, hash_password(password), email, city, tariff, ref_code, referrer),
    )
    db.commit()
    pid = db.execute('SELECT last_insert_rowid()').fetchone()[0]

    send_telegram(
        f'🦅 <b>Новый партнёр TULPAR!</b>\n\n'
        f'👤 Имя: {name}\n'
        f'📱 Телефон: +{phone}\n'
        f'🏙 Город: {city or "не указан"}\n'
        f'💼 Тариф: Бесплатный (пакет не куплен)\n'
        f'🔗 Реф. код: {ref_code}'
        + (f'\n👥 Реферер: {referrer}' if referrer else '')
        + f'\n⏰ {datetime.now().strftime("%d.%m.%Y %H:%M")}'
    )

    return jsonify({'success': True, 'ref_code': ref_code, 'id': pid, 'message': 'Регистрация успешна!'})


# ─── API: LOGIN ──────────────────────────────────────────────────────────

@app.post('/api/login')
def api_login():
    d = request.get_json(force=True) or {}
    phone = ''.join(c for c in (d.get('phone') or '') if c.isdigit())
    password = (d.get('password') or '').strip()

    if not phone or not password:
        return jsonify({'error': 'Укажите телефон и пароль'}), 400

    db = get_db()
    row = db.execute('SELECT * FROM partners WHERE phone = ?', (phone,)).fetchone()
    if not row:
        return jsonify({'error': 'Партнёр не найден. Сначала зарегистрируйтесь.'}), 404

    p = dict(row)

    if not check_password(p.get('password', ''), password):
        return jsonify({'error': 'Неверный пароль'}), 401

    token = create_token(p['id'])

    return jsonify({
        'success': True,
        'token': token,
        'partner': {
            'id': p['id'],
            'name': p['name'],
            'tariff': p['tariff'],
            'ref_code': p['ref_code'],
        },
    })


# ─── API: ADMIN LOGIN ───────────────────────────────────────────────────

@app.post('/api/admin/login')
def api_admin_login():
    d = request.get_json(force=True) or {}
    if d.get('password') != ADMIN_PASSWORD:
        return jsonify({'error': 'Неверный пароль'}), 401
    token = create_token(0, is_admin=True)
    return jsonify({'success': True, 'token': token})


# ─── API: PASSWORD RESET ─────────────────────────────────────────────────

@app.post('/api/reset-password')
def api_reset_password():
    d = request.get_json(force=True) or {}
    phone = ''.join(c for c in (d.get('phone') or '') if c.isdigit())

    if not phone:
        return jsonify({'error': 'Укажите телефон'}), 400

    db = get_db()
    row = db.execute('SELECT * FROM partners WHERE phone = ?', (phone,)).fetchone()
    if not row:
        return jsonify({'error': 'Партнёр с таким номером не найден'}), 404

    p = dict(row)
    new_password = secrets.token_hex(4)  # 8-char temp password
    db.execute(
        'UPDATE partners SET password = ? WHERE id = ?',
        (hash_password(new_password), p['id']),
    )
    db.commit()

    send_telegram(
        f'🔑 <b>Запрос на сброс пароля</b>\n\n'
        f'👤 Партнёр: {p["name"]}\n'
        f'📱 Телефон: +{phone}\n'
        f'🔒 Новый пароль: <code>{new_password}</code>\n\n'
        f'Передайте пароль партнёру и попросите сменить после входа.'
    )

    return jsonify({
        'success': True,
        'message': 'Новый пароль отправлен администратору в Telegram. Он свяжется с вами.',
    })


# ─── API: CHANGE PASSWORD ────────────────────────────────────────────────

@app.post('/api/partner/change-password')
@auth_required
def api_change_password():
    d = request.get_json(force=True) or {}
    old_pw = (d.get('old_password') or '').strip()
    new_pw = (d.get('new_password') or '').strip()

    if not old_pw or not new_pw:
        return jsonify({'error': 'Заполните оба поля'}), 400
    if len(new_pw) < 4:
        return jsonify({'error': 'Новый пароль должен быть минимум 4 символа'}), 400

    db = get_db()
    row = db.execute('SELECT password FROM partners WHERE id = ?', (request.partner_id,)).fetchone()
    if not row or not check_password(row['password'], old_pw):
        return jsonify({'error': 'Неверный текущий пароль'}), 401

    db.execute(
        'UPDATE partners SET password = ? WHERE id = ?',
        (hash_password(new_pw), request.partner_id),
    )
    db.commit()
    return jsonify({'success': True, 'message': 'Пароль успешно изменён'})


# ─── API: BUY TARIFF ─────────────────────────────────────────────────────

def _credit_ref_bonus(db, buyer_id, buyer_name, tariff, price):
    """Credit referral bonuses up to 2 levels."""
    buyer = db.execute('SELECT referrer FROM partners WHERE id = ?', (buyer_id,)).fetchone()
    if not buyer or not buyer['referrer']:
        return

    # Level 1 referrer
    ref1 = db.execute('SELECT * FROM partners WHERE ref_code = ?', (buyer['referrer'],)).fetchone()
    if ref1 and ref1['tariff'] in ('partner', 'leader'):
        bonus1 = int(price * REF_BONUS_L1)
        if bonus1 > 0:
            db.execute('UPDATE partners SET balance = balance + ? WHERE id = ?', (bonus1, ref1['id']))
            db.execute(
                'INSERT INTO transactions (partner_id, type, amount, description, related_partner_id) '
                'VALUES (?, ?, ?, ?, ?)',
                (ref1['id'], 'ref_bonus_l1', bonus1,
                 f'Бонус 3% от пакета {tariff.upper()} ({buyer_name})', buyer_id),
            )
            send_telegram(
                f'💰 <b>Реферальный бонус!</b>\n'
                f'👤 {ref1["name"]} получил <b>{bonus1:,} ₸</b> (3% от пакета {buyer_name})'
            )

        # Level 2 referrer
        if ref1['referrer']:
            ref2 = db.execute('SELECT * FROM partners WHERE ref_code = ?', (ref1['referrer'],)).fetchone()
            if ref2 and ref2['tariff'] in ('partner', 'leader'):
                bonus2 = int(price * REF_BONUS_L2)
                if bonus2 > 0:
                    db.execute('UPDATE partners SET balance = balance + ? WHERE id = ?', (bonus2, ref2['id']))
                    db.execute(
                        'INSERT INTO transactions (partner_id, type, amount, description, related_partner_id) '
                        'VALUES (?, ?, ?, ?, ?)',
                        (ref2['id'], 'ref_bonus_l2', bonus2,
                         f'Бонус 1% (2 ур.) от пакета {tariff.upper()} ({buyer_name})', buyer_id),
                    )


@app.post('/api/partner/buy-tariff')
@auth_required
def api_buy_tariff():
    d = request.get_json(force=True) or {}
    tariff = (d.get('tariff') or '').strip()

    if tariff not in TARIFF_PRICES:
        return jsonify({'error': 'Неверный тариф'}), 400

    db = get_db()
    p = db.execute('SELECT * FROM partners WHERE id = ?', (request.partner_id,)).fetchone()
    if not p:
        return jsonify({'error': 'Партнёр не найден'}), 404

    current = p['tariff']
    if current == tariff:
        return jsonify({'error': 'У вас уже этот тариф'}), 400
    if current != 'free' and TARIFF_PRICES.get(current, 0) >= TARIFF_PRICES[tariff]:
        return jsonify({'error': 'Можно только повысить тариф'}), 400

    price = TARIFF_PRICES[tariff]

    # Update partner tariff
    db.execute('UPDATE partners SET tariff = ?, status = ? WHERE id = ?', (tariff, 'paid', request.partner_id))

    # Record platform income transaction
    db.execute(
        'INSERT INTO transactions (partner_id, type, amount, description) VALUES (?, ?, ?, ?)',
        (request.partner_id, 'package_purchase', price, f'Покупка пакета {TARIFF_NAMES[tariff]}'),
    )

    # Credit referral bonuses
    _credit_ref_bonus(db, request.partner_id, p['name'], tariff, price)

    db.commit()

    send_telegram(
        f'🎉 <b>Покупка пакета!</b>\n\n'
        f'👤 {p["name"]}\n'
        f'💼 Пакет: {TARIFF_NAMES[tariff]}\n'
        f'💰 Сумма: {price:,} ₸'
    )

    return jsonify({'success': True, 'tariff': tariff, 'message': f'Пакет {TARIFF_NAMES[tariff]} активирован!'})


@app.get('/api/partner/transactions')
@auth_required
def api_partner_transactions():
    db = get_db()
    rows = [dict(r) for r in db.execute(
        'SELECT * FROM transactions WHERE partner_id = ? ORDER BY created_at DESC LIMIT 50',
        (request.partner_id,),
    ).fetchall()]
    return jsonify(rows)


# ─── API: PARTNER ────────────────────────────────────────────────────────

@app.get('/api/partner/me')
@auth_required
def api_partner_me():
    db = get_db()
    row = db.execute('SELECT * FROM partners WHERE id = ?', (request.partner_id,)).fetchone()
    if not row:
        return jsonify({'error': 'Партнёр не найден'}), 404

    p = dict(row)
    team_count = db.execute(
        'SELECT COUNT(*) FROM partners WHERE referrer = ?', (p['ref_code'],)
    ).fetchone()[0]
    p['teamCount'] = team_count

    return jsonify(p)


# ─── API: ADMIN — PARTNERS ──────────────────────────────────────────────

@app.get('/api/admin/partners')
@admin_required
def api_admin_partners():
    db = get_db()
    query = 'SELECT * FROM partners WHERE 1=1'
    params = []

    search = request.args.get('search', '')
    tariff = request.args.get('tariff', '')
    status = request.args.get('status', '')

    if search:
        query += ' AND (name LIKE ? OR phone LIKE ? OR city LIKE ?)'
        params.extend([f'%{search}%'] * 3)
    if tariff:
        query += ' AND tariff = ?'
        params.append(tariff)
    if status:
        query += ' AND status = ?'
        params.append(status)

    query += ' ORDER BY created_at DESC'
    rows = [dict(r) for r in db.execute(query, params).fetchall()]

    return jsonify(rows)


# ─── API: ADMIN — STATS ─────────────────────────────────────────────────

@app.get('/api/admin/stats')
@admin_required
def api_admin_stats():
    db = get_db()

    total = db.execute('SELECT COUNT(*) FROM partners').fetchone()[0]
    today = db.execute(
        "SELECT COUNT(*) FROM partners WHERE date(created_at) = date('now')"
    ).fetchone()[0]
    week = db.execute(
        "SELECT COUNT(*) FROM partners WHERE created_at >= datetime('now', '-7 days')"
    ).fetchone()[0]

    by_tariff = [dict(r) for r in db.execute(
        'SELECT tariff, COUNT(*) as c FROM partners GROUP BY tariff'
    ).fetchall()]
    by_status = [dict(r) for r in db.execute(
        'SELECT status, COUNT(*) as c FROM partners GROUP BY status'
    ).fetchall()]

    revenue = {}
    for key, price in TARIFF_PRICES.items():
        count = db.execute(
            'SELECT COUNT(*) FROM partners WHERE tariff = ?', (key,)
        ).fetchone()[0]
        revenue[key] = count * price
    revenue['total'] = sum(revenue.values())

    return jsonify({
        'total': total,
        'today': today,
        'week': week,
        'byTariff': by_tariff,
        'byStatus': by_status,
        'revenue': revenue,
    })


# ─── API: ADMIN — UPDATE PARTNER ────────────────────────────────────────

@app.patch('/api/admin/partner/<int:pid>')
@admin_required
def api_update_partner(pid):
    d = request.get_json(force=True) or {}
    db = get_db()

    if 'status' in d:
        db.execute('UPDATE partners SET status = ? WHERE id = ?', (d['status'], pid))
    if 'notes' in d:
        db.execute('UPDATE partners SET notes = ? WHERE id = ?', (d['notes'], pid))

    db.commit()
    return jsonify({'success': True})


# ─── API: ADMIN — DELETE PARTNER ─────────────────────────────────────────

@app.delete('/api/admin/partner/<int:pid>')
@admin_required
def api_delete_partner(pid):
    db = get_db()
    db.execute('DELETE FROM partners WHERE id = ?', (pid,))
    db.commit()
    return jsonify({'success': True})


# ─── API: ADMIN — EXPORT CSV ────────────────────────────────────────────

@app.get('/api/admin/export')
@admin_required
def api_export():
    db = get_db()
    rows = db.execute('SELECT * FROM partners ORDER BY created_at DESC').fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Имя', 'Телефон', 'Email', 'Город', 'Тариф', 'Реф.код', 'Реферер', 'Статус', 'Дата'])

    for r in rows:
        r = dict(r)
        writer.writerow([
            r['id'], r['name'], r['phone'], r['email'],
            r['city'], r['tariff'], r['ref_code'], r['referrer'],
            r['status'], r['created_at'],
        ])

    csv_content = '\ufeff' + output.getvalue()
    return Response(
        csv_content,
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': 'attachment; filename=tulpar_partners.csv'},
    )


# ─── API: CATALOG — PUBLIC ───────────────────────────────────────────────

@app.get('/api/categories')
def api_categories():
    db = get_db()
    rows = [dict(r) for r in db.execute('SELECT * FROM categories ORDER BY sort_order').fetchall()]
    return jsonify(rows)


@app.get('/api/products')
def api_products():
    db = get_db()
    category_id = request.args.get('category')
    if category_id:
        rows = db.execute(
            'SELECT * FROM products WHERE in_stock = 1 AND category_id = ? ORDER BY created_at DESC',
            (category_id,),
        ).fetchall()
    else:
        rows = db.execute(
            'SELECT * FROM products WHERE in_stock = 1 ORDER BY created_at DESC'
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.get('/api/products/<int:pid>')
def api_product_detail(pid):
    db = get_db()
    row = db.execute('SELECT * FROM products WHERE id = ?', (pid,)).fetchone()
    if not row:
        return jsonify({'error': 'Товар не найден'}), 404
    return jsonify(dict(row))


# ─── API: ORDERS ─────────────────────────────────────────────────────────

@app.post('/api/orders')
def api_create_order():
    d = request.get_json(force=True) or {}
    name = (d.get('name') or '').strip()
    phone = ''.join(c for c in (d.get('phone') or '') if c.isdigit())
    items = d.get('items') or []
    partner_ref = (d.get('partner_ref') or '').strip().upper()

    if not name or not phone or not items:
        return jsonify({'error': 'Заполните имя, телефон и добавьте товары'}), 400

    db = get_db()
    total = 0
    order_lines = []

    for item in items:
        product = db.execute('SELECT * FROM products WHERE id = ?', (item.get('id'),)).fetchone()
        if not product:
            continue
        qty = max(1, int(item.get('quantity', 1)))
        line_total = product['price'] * qty
        total += line_total
        order_lines.append((product['id'], qty, product['price'], product['name']))

    if not order_lines:
        return jsonify({'error': 'Нет валидных товаров в заказе'}), 400

    db.execute(
        'INSERT INTO orders (partner_ref, customer_name, customer_phone, status, total) VALUES (?, ?, ?, ?, ?)',
        (partner_ref, name, phone, 'new', total),
    )
    db.commit()
    order_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]

    for product_id, qty, price, _ in order_lines:
        db.execute(
            'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)',
            (order_id, product_id, qty, price),
        )
    db.commit()

    items_text = '\n'.join(f'  - {name} x{qty} = {price * qty:,} ₸' for pid, qty, price, name in order_lines)
    send_telegram(
        f'🛒 <b>Новый заказ #{order_id}!</b>\n\n'
        f'👤 {name}\n📱 +{phone}\n'
        f'📦 Товары:\n{items_text}\n'
        f'💰 Итого: <b>{total:,} ₸</b>'
        + (f'\n🔗 Партнёр: {partner_ref}' if partner_ref else '')
    )

    # Credit order commission to partner
    if partner_ref:
        ref_partner = db.execute('SELECT * FROM partners WHERE ref_code = ?', (partner_ref,)).fetchone()
        if ref_partner and ref_partner['tariff'] in TARIFF_ORDER_COMM:
            comm_rate = TARIFF_ORDER_COMM[ref_partner['tariff']]
            commission = int(total * comm_rate)
            if commission > 0:
                db.execute('UPDATE partners SET balance = balance + ? WHERE id = ?', (commission, ref_partner['id']))
                db.execute(
                    'INSERT INTO transactions (partner_id, type, amount, description) VALUES (?, ?, ?, ?)',
                    (ref_partner['id'], 'order_commission', commission,
                     f'Комиссия {int(comm_rate*100)}% с заказа #{order_id} ({total:,} ₸)'),
                )
                db.commit()

    return jsonify({'success': True, 'order_id': order_id, 'total': total})


# ─── API: ADMIN — PRODUCTS ──────────────────────────────────────────────

@app.post('/api/admin/products')
@admin_required
def api_add_product():
    d = request.get_json(force=True) or {}
    name = (d.get('name') or '').strip()
    price = int(d.get('price') or 0)
    category_id = int(d.get('category_id') or 0)
    description = (d.get('description') or '').strip()
    image_url = (d.get('image_url') or '').strip()

    if not name or not price:
        return jsonify({'error': 'Укажите название и цену'}), 400

    db = get_db()
    db.execute(
        'INSERT INTO products (category_id, name, description, price, image_url) VALUES (?, ?, ?, ?, ?)',
        (category_id, name, description, price, image_url),
    )
    db.commit()
    pid = db.execute('SELECT last_insert_rowid()').fetchone()[0]
    return jsonify({'success': True, 'id': pid})


@app.get('/api/admin/products')
@admin_required
def api_admin_products():
    db = get_db()
    rows = [dict(r) for r in db.execute('SELECT * FROM products ORDER BY created_at DESC').fetchall()]
    return jsonify(rows)


@app.patch('/api/admin/products/<int:pid>')
@admin_required
def api_update_product(pid):
    d = request.get_json(force=True) or {}
    db = get_db()
    fields = []
    values = []
    for key in ('name', 'description', 'image_url'):
        if key in d:
            fields.append(f'{key} = ?')
            values.append(d[key])
    for key in ('price', 'category_id', 'in_stock'):
        if key in d:
            fields.append(f'{key} = ?')
            values.append(int(d[key]))
    if fields:
        values.append(pid)
        db.execute(f'UPDATE products SET {", ".join(fields)} WHERE id = ?', values)
        db.commit()
    return jsonify({'success': True})


@app.delete('/api/admin/products/<int:pid>')
@admin_required
def api_delete_product(pid):
    db = get_db()
    db.execute('DELETE FROM products WHERE id = ?', (pid,))
    db.commit()
    return jsonify({'success': True})


# ─── API: ADMIN — ORDERS ────────────────────────────────────────────────

@app.get('/api/admin/orders')
@admin_required
def api_admin_orders():
    db = get_db()
    rows = [dict(r) for r in db.execute('SELECT * FROM orders ORDER BY created_at DESC').fetchall()]
    for order in rows:
        items = [dict(r) for r in db.execute(
            'SELECT oi.*, p.name as product_name FROM order_items oi '
            'JOIN products p ON p.id = oi.product_id WHERE oi.order_id = ?',
            (order['id'],),
        ).fetchall()]
        order['items'] = items
    return jsonify(rows)


@app.patch('/api/admin/orders/<int:oid>')
@admin_required
def api_update_order(oid):
    d = request.get_json(force=True) or {}
    if 'status' in d:
        db = get_db()
        db.execute('UPDATE orders SET status = ? WHERE id = ?', (d['status'], oid))
        db.commit()
    return jsonify({'success': True})


# ─── API: PARTNER — ORDERS COUNT ────────────────────────────────────────

@app.get('/api/partner/orders')
@auth_required
def api_partner_orders():
    db = get_db()
    p = db.execute('SELECT ref_code FROM partners WHERE id = ?', (request.partner_id,)).fetchone()
    if not p:
        return jsonify({'count': 0, 'total': 0})
    ref = p['ref_code']
    count = db.execute('SELECT COUNT(*) FROM orders WHERE partner_ref = ?', (ref,)).fetchone()[0]
    total = db.execute('SELECT COALESCE(SUM(total), 0) FROM orders WHERE partner_ref = ?', (ref,)).fetchone()[0]
    return jsonify({'count': count, 'total': total})


# ─── API: ADMIN — FINANCES (CRM) ────────────────────────────────────────

@app.get('/api/admin/finances')
@admin_required
def api_admin_finances():
    db = get_db()

    def sum_by_type(t, period=''):
        q = f"SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE type = '{t}'"
        if period == 'today':
            q += " AND date(created_at) = date('now')"
        elif period == 'week':
            q += " AND created_at >= datetime('now', '-7 days')"
        elif period == 'month':
            q += " AND created_at >= datetime('now', '-30 days')"
        return db.execute(q).fetchone()[0]

    packages = {
        'today': sum_by_type('package_purchase', 'today'),
        'week': sum_by_type('package_purchase', 'week'),
        'month': sum_by_type('package_purchase', 'month'),
        'total': sum_by_type('package_purchase'),
    }

    orders_income = {
        'today': db.execute("SELECT COALESCE(SUM(total),0) FROM orders WHERE date(created_at)=date('now')").fetchone()[0],
        'week': db.execute("SELECT COALESCE(SUM(total),0) FROM orders WHERE created_at>=datetime('now','-7 days')").fetchone()[0],
        'month': db.execute("SELECT COALESCE(SUM(total),0) FROM orders WHERE created_at>=datetime('now','-30 days')").fetchone()[0],
        'total': db.execute("SELECT COALESCE(SUM(total),0) FROM orders").fetchone()[0],
    }

    bonuses_paid = {
        'total': db.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type IN ('ref_bonus_l1','ref_bonus_l2','order_commission')").fetchone()[0],
    }

    orders_count = db.execute('SELECT COUNT(*) FROM orders').fetchone()[0]
    orders_today = db.execute("SELECT COUNT(*) FROM orders WHERE date(created_at)=date('now')").fetchone()[0]

    return jsonify({
        'packages': packages,
        'orders_income': orders_income,
        'bonuses_paid': bonuses_paid,
        'orders_count': orders_count,
        'orders_today': orders_today,
    })


@app.get('/api/admin/transactions')
@admin_required
def api_admin_transactions():
    db = get_db()
    rows = db.execute(
        'SELECT t.*, p.name as partner_name FROM transactions t '
        'LEFT JOIN partners p ON p.id = t.partner_id '
        'ORDER BY t.created_at DESC LIMIT 100'
    ).fetchall()
    return jsonify([dict(r) for r in rows])


# ─── START ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print(f'\n  TULPAR запущен!\n')
    print(f'  Сайт:        http://localhost:{PORT}')
    print(f'  Регистрация:  http://localhost:{PORT}/register')
    print(f'  Вход:         http://localhost:{PORT}/login')
    print(f'  Кабинет:      http://localhost:{PORT}/dashboard')
    print(f'  Админ:        http://localhost:{PORT}/admin')
    print(f'\n  Пароль админа: {ADMIN_PASSWORD}\n')

    app.run(host='0.0.0.0', port=PORT, debug=False)
