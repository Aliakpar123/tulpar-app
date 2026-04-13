#!/usr/bin/env python3
"""TULPAR — Social Commerce Platform Server"""

import csv
import hashlib
import hmac
import io
import json as json_module
import os
import secrets
import sqlite3
import urllib.parse
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

# Level system — auto-promoted by team size
LEVEL_THRESHOLDS = {'manager': 10, 'director': 50}
LEVEL_COMM = {
    'manager': {'order': 0.15, 'l1': 0.04, 'l2': 0.02},
    'director': {'order': 0.18, 'l1': 0.05, 'l2': 0.03},
}
LEVEL_NAMES = {'member': 'Участник', 'manager': 'Менеджер', 'director': 'Директор'}
TG_BOT_USERNAME = os.environ.get('TELEGRAM_BOT_USERNAME', 'tulpar_kz_bot')

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

            CREATE TABLE IF NOT EXISTS partner_shop_products (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_id  INTEGER NOT NULL,
                product_id  INTEGER NOT NULL,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(partner_id, product_id)
            );

            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id  INTEGER NOT NULL,
                customer_name TEXT NOT NULL,
                rating      INTEGER NOT NULL,
                text        TEXT    DEFAULT '',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS payouts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_id  INTEGER NOT NULL,
                amount      INTEGER NOT NULL,
                status      TEXT    DEFAULT 'pending',
                notes       TEXT    DEFAULT '',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS checkins (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_id  INTEGER NOT NULL,
                date        TEXT    NOT NULL,
                streak      INTEGER DEFAULT 1,
                coins_earned INTEGER DEFAULT 10,
                UNIQUE(partner_id, date)
            );

            CREATE TABLE IF NOT EXISTS shared_carts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT    UNIQUE NOT NULL,
                items       TEXT    NOT NULL,
                partner_ref TEXT    DEFAULT '',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Migrate existing tables
        for col_sql in [
            "ALTER TABLE partners ADD COLUMN password TEXT DEFAULT ''",
            "ALTER TABLE partners ADD COLUMN balance INTEGER DEFAULT 0",
            "ALTER TABLE partners ADD COLUMN level TEXT DEFAULT 'member'",
            "ALTER TABLE partners ADD COLUMN telegram_chat_id TEXT DEFAULT ''",
            "ALTER TABLE partners ADD COLUMN coins INTEGER DEFAULT 0",
            "ALTER TABLE partners ADD COLUMN last_checkin TEXT DEFAULT ''",
            "ALTER TABLE partners ADD COLUMN checkin_streak INTEGER DEFAULT 0",
            "ALTER TABLE products ADD COLUMN partner_price INTEGER DEFAULT 0",
            "ALTER TABLE products ADD COLUMN is_selected INTEGER DEFAULT 0",
        ]:
            try:
                conn.execute(col_sql)
            except sqlite3.OperationalError:
                pass
        try:
            conn.execute('CREATE INDEX IF NOT EXISTS idx_partners_referrer ON partners(referrer)')
        except Exception:
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


def send_partner_telegram(partner_id, text):
    """Send notification to partner's personal Telegram if linked."""
    if not TG_TOKEN or TG_TOKEN == 'your_telegram_bot_token_here':
        return
    try:
        db = get_db()
        row = db.execute('SELECT telegram_chat_id FROM partners WHERE id = ?', (partner_id,)).fetchone()
        if not row or not row['telegram_chat_id']:
            return
        http_requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            json={'chat_id': row['telegram_chat_id'], 'text': text, 'parse_mode': 'HTML'},
            timeout=5,
        )
    except Exception as e:
        print(f'Partner TG error: {e}')


def check_and_update_level(db, partner_id):
    """Auto-promote partner based on team size."""
    row = db.execute('SELECT ref_code, level FROM partners WHERE id = ?', (partner_id,)).fetchone()
    if not row:
        return 'member'
    team_count = db.execute('SELECT COUNT(*) FROM partners WHERE referrer = ?', (row['ref_code'],)).fetchone()[0]
    new_level = 'member'
    if team_count >= LEVEL_THRESHOLDS['director']:
        new_level = 'director'
    elif team_count >= LEVEL_THRESHOLDS['manager']:
        new_level = 'manager'
    if new_level != row['level']:
        db.execute('UPDATE partners SET level = ? WHERE id = ?', (new_level, partner_id))
        db.commit()
        if new_level != 'member':
            send_partner_telegram(partner_id,
                f'🎉 <b>Поздравляем!</b>\nВы достигли уровня <b>{LEVEL_NAMES[new_level]}</b>!\n'
                f'Новые комиссии: {int(LEVEL_COMM[new_level]["order"]*100)}% с заказов')
    return new_level


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


@app.route('/shop/<ref_code>')
def shop_page(ref_code):
    return send_from_directory(app.static_folder, 'shop.html')


@app.route('/tg-app')
def tg_app_page():
    return send_from_directory(app.static_folder, 'tg-app.html')


# ─── API: TELEGRAM AUTH ─────────────────────────────────────────────────

def validate_telegram_init_data(init_data_raw):
    """Validate Telegram WebApp initData using HMAC-SHA-256."""
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data_raw, keep_blank_values=True))
        received_hash = parsed.pop('hash', '')
        if not received_hash:
            return None
        data_check_string = '\n'.join(f'{k}={v}' for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b'WebAppData', TG_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calculated_hash != received_hash:
            return None
        user_data = json_module.loads(parsed.get('user', '{}'))
        return user_data
    except Exception:
        return None


@app.post('/api/tg-auth')
def api_tg_auth():
    d = request.get_json(force=True) or {}
    init_data = d.get('initData', '')

    # Validate initData
    user = validate_telegram_init_data(init_data)
    if not user:
        return jsonify({'error': 'Невалидные данные Telegram'}), 401

    tg_id = str(user.get('id', ''))
    if not tg_id:
        return jsonify({'error': 'Нет Telegram ID'}), 401

    db = get_db()

    # Find partner by telegram_chat_id
    partner = db.execute('SELECT * FROM partners WHERE telegram_chat_id = ?', (tg_id,)).fetchone()

    if not partner:
        # Auto-register from Telegram
        first_name = user.get('first_name', 'Пользователь')
        last_name = user.get('last_name', '')
        name = f'{first_name} {last_name}'.strip()
        username = user.get('username', '')
        ref_code = 'TLP' + secrets.token_hex(3).upper()

        # Check start_param for referrer
        start_param = d.get('start_param', '')

        db.execute(
            'INSERT INTO partners (name, phone, password, tariff, ref_code, referrer, telegram_chat_id, status) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (name, '', '', 'free', ref_code, start_param.upper() if start_param else '', tg_id, 'new'),
        )
        db.commit()
        partner = db.execute('SELECT * FROM partners WHERE telegram_chat_id = ?', (tg_id,)).fetchone()

        send_telegram(
            f'🦅 <b>Новый партнёр через Mini App!</b>\n'
            f'👤 {name}\n💬 @{username}\n🔗 Код: {ref_code}'
        )

    p = dict(partner)
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

    # Check if referrer should level up + notify them
    if referrer:
        ref_partner = db.execute('SELECT id FROM partners WHERE ref_code = ?', (referrer,)).fetchone()
        if ref_partner:
            check_and_update_level(db, ref_partner['id'])
            send_partner_telegram(ref_partner['id'],
                f'👥 <b>Новый участник в команде!</b>\n{name} присоединился по вашей ссылке')

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
    """Credit referral bonuses up to 2 levels, using level-based rates if higher."""
    buyer = db.execute('SELECT referrer FROM partners WHERE id = ?', (buyer_id,)).fetchone()
    if not buyer or not buyer['referrer']:
        return

    # Level 1 referrer
    ref1 = db.execute('SELECT * FROM partners WHERE ref_code = ?', (buyer['referrer'],)).fetchone()
    if ref1 and ref1['tariff'] in ('partner', 'leader'):
        l1_rate = REF_BONUS_L1
        level1 = ref1['level'] if 'level' in ref1.keys() else 'member'
        if level1 in LEVEL_COMM:
            l1_rate = max(l1_rate, LEVEL_COMM[level1]['l1'])
        bonus1 = int(price * l1_rate)
        if bonus1 > 0:
            db.execute('UPDATE partners SET balance = balance + ? WHERE id = ?', (bonus1, ref1['id']))
            db.execute(
                'INSERT INTO transactions (partner_id, type, amount, description, related_partner_id) '
                'VALUES (?, ?, ?, ?, ?)',
                (ref1['id'], 'ref_bonus_l1', bonus1,
                 f'Бонус {int(l1_rate*100)}% от пакета {tariff.upper()} ({buyer_name})', buyer_id),
            )
            send_telegram(
                f'💰 <b>Реферальный бонус!</b>\n'
                f'👤 {ref1["name"]} получил <b>{bonus1:,} ₸</b> ({int(l1_rate*100)}% от пакета {buyer_name})'
            )
            send_partner_telegram(ref1['id'],
                f'💰 <b>+{bonus1:,} ₸ бонус!</b>\n{buyer_name} купил пакет {tariff.upper()}')

        # Level 2 referrer
        if ref1['referrer']:
            ref2 = db.execute('SELECT * FROM partners WHERE ref_code = ?', (ref1['referrer'],)).fetchone()
            if ref2 and ref2['tariff'] in ('partner', 'leader'):
                l2_rate = REF_BONUS_L2
                level2 = ref2['level'] if 'level' in ref2.keys() else 'member'
                if level2 in LEVEL_COMM:
                    l2_rate = max(l2_rate, LEVEL_COMM[level2]['l2'])
                bonus2 = int(price * l2_rate)
                if bonus2 > 0:
                    db.execute('UPDATE partners SET balance = balance + ? WHERE id = ?', (bonus2, ref2['id']))
                    db.execute(
                        'INSERT INTO transactions (partner_id, type, amount, description, related_partner_id) '
                        'VALUES (?, ?, ?, ?, ?)',
                        (ref2['id'], 'ref_bonus_l2', bonus2,
                         f'Бонус {int(l2_rate*100)}% (2 ур.) от пакета {tariff.upper()} ({buyer_name})', buyer_id),
                    )
                    send_partner_telegram(ref2['id'],
                        f'💰 <b>+{bonus2:,} ₸ бонус (2 ур.)!</b>\n{buyer_name} купил пакет')


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

    # Level system
    level = check_and_update_level(db, request.partner_id)
    p['level'] = level
    p['levelName'] = LEVEL_NAMES.get(level, 'Участник')
    if level == 'member':
        p['nextLevelAt'] = LEVEL_THRESHOLDS['manager']
    elif level == 'manager':
        p['nextLevelAt'] = LEVEL_THRESHOLDS['director']
    else:
        p['nextLevelAt'] = None

    # Effective commissions (level overrides tariff if higher)
    tariff_comm = TARIFF_ORDER_COMM.get(p.get('tariff'), 0)
    level_comm = LEVEL_COMM.get(level, {}).get('order', 0)
    p['effectiveComm'] = max(tariff_comm, level_comm)

    p['botUsername'] = TG_BOT_USERNAME

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

    # Credit order commission to partner (use level rate if higher)
    if partner_ref:
        ref_partner = db.execute('SELECT * FROM partners WHERE ref_code = ?', (partner_ref,)).fetchone()
        if ref_partner and ref_partner['tariff'] in TARIFF_ORDER_COMM:
            comm_rate = TARIFF_ORDER_COMM[ref_partner['tariff']]
            p_level = ref_partner['level'] if 'level' in ref_partner.keys() else 'member'
            if p_level in LEVEL_COMM and LEVEL_COMM[p_level]['order'] > comm_rate:
                comm_rate = LEVEL_COMM[p_level]['order']
            commission = int(total * comm_rate)
            if commission > 0:
                db.execute('UPDATE partners SET balance = balance + ? WHERE id = ?', (commission, ref_partner['id']))
                db.execute(
                    'INSERT INTO transactions (partner_id, type, amount, description) VALUES (?, ?, ?, ?)',
                    (ref_partner['id'], 'order_commission', commission,
                     f'Комиссия {int(comm_rate*100)}% с заказа #{order_id} ({total:,} ₸)'),
                )
                db.commit()
                send_partner_telegram(ref_partner['id'],
                    f'🛒 <b>Новый заказ #{order_id}!</b>\nСумма: {total:,} ₸\nВаша комиссия: <b>+{commission:,} ₸</b>')

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


# ─── API: SHOP ───────────────────────────────────────────────────────────

@app.get('/api/shop/<ref_code>')
def api_shop(ref_code):
    db = get_db()
    partner = db.execute(
        'SELECT name, ref_code, city, level FROM partners WHERE ref_code = ?', (ref_code,)
    ).fetchone()
    if not partner:
        return jsonify({'error': 'Магазин не найден'}), 404
    p = dict(partner)
    p['levelName'] = LEVEL_NAMES.get(p.get('level', 'member'), 'Участник')
    pid = db.execute('SELECT id FROM partners WHERE ref_code = ?', (ref_code,)).fetchone()['id']
    products = [dict(r) for r in db.execute(
        'SELECT p.* FROM products p JOIN partner_shop_products sp ON sp.product_id = p.id '
        'WHERE sp.partner_id = ? AND p.in_stock = 1', (pid,)
    ).fetchall()]
    # If partner has no products selected, show all
    if not products:
        products = [dict(r) for r in db.execute(
            'SELECT * FROM products WHERE in_stock = 1 ORDER BY created_at DESC'
        ).fetchall()]
    return jsonify({'partner': p, 'products': products})


@app.get('/api/partner/shop-products')
@auth_required
def api_get_shop_products():
    db = get_db()
    rows = db.execute(
        'SELECT product_id FROM partner_shop_products WHERE partner_id = ?', (request.partner_id,)
    ).fetchall()
    return jsonify([r['product_id'] for r in rows])


@app.post('/api/partner/shop-products')
@auth_required
def api_set_shop_products():
    d = request.get_json(force=True) or {}
    product_ids = d.get('product_ids', [])
    db = get_db()
    db.execute('DELETE FROM partner_shop_products WHERE partner_id = ?', (request.partner_id,))
    for pid in product_ids:
        db.execute(
            'INSERT OR IGNORE INTO partner_shop_products (partner_id, product_id) VALUES (?, ?)',
            (request.partner_id, int(pid)),
        )
    db.commit()
    return jsonify({'success': True, 'count': len(product_ids)})


# ─── API: LEADERBOARD ───────────────────────────────────────────────────

@app.get('/api/leaderboard')
def api_leaderboard():
    db = get_db()
    rows = db.execute('''
        SELECT p.name, p.city, p.ref_code, p.level,
               (SELECT COUNT(*) FROM partners r WHERE r.referrer = p.ref_code) as team_count
        FROM partners p
        WHERE p.tariff != 'free'
        ORDER BY team_count DESC
        LIMIT 10
    ''').fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['levelName'] = LEVEL_NAMES.get(d.get('level', 'member'), 'Участник')
        result.append(d)
    return jsonify(result)


# ─── API: TELEGRAM BOT WEBHOOK ──────────────────────────────────────────

@app.post('/api/telegram/webhook')
def api_telegram_webhook():
    data = request.get_json(force=True) or {}
    message = data.get('message', {})
    chat_id = str(message.get('chat', {}).get('id', ''))
    text = (message.get('text') or '').strip()

    if not chat_id or not text:
        return jsonify({'ok': True})

    db = get_db()
    APP_URL = request.host_url.rstrip('/')

    def reply(msg, keyboard=None):
        payload = {'chat_id': chat_id, 'text': msg, 'parse_mode': 'HTML'}
        if keyboard:
            payload['reply_markup'] = keyboard
        try:
            http_requests.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', json=payload, timeout=5)
        except Exception:
            pass

    # /start — with or without ref code
    if text.startswith('/start'):
        parts = text.split()
        ref_code = parts[1].upper() if len(parts) > 1 else ''

        # Link account if ref_code provided
        if ref_code:
            partner = db.execute('SELECT id, name FROM partners WHERE ref_code = ?', (ref_code,)).fetchone()
            if partner:
                db.execute('UPDATE partners SET telegram_chat_id = ? WHERE id = ?', (chat_id, partner['id']))
                db.commit()

        # Send welcome with Mini App button
        reply(
            '🦅 <b>Добро пожаловать в TULPAR!</b>\n\n'
            'Зарабатывай продавая то, что любишь.\n'
            'Нажми кнопку ниже чтобы открыть приложение.',
            {'inline_keyboard': [[{
                'text': '🚀 Открыть TULPAR',
                'web_app': {'url': f'{APP_URL}/tg-app'}
            }]]}
        )
        return jsonify({'ok': True})

    # /balance
    if text == '/balance':
        partner = db.execute('SELECT name, balance, level FROM partners WHERE telegram_chat_id = ?', (chat_id,)).fetchone()
        if partner:
            reply(f'💰 <b>{partner["name"]}</b>\n\nБаланс: <b>{partner["balance"]:,} ₸</b>\nУровень: {LEVEL_NAMES.get(partner["level"], "Участник")}')
        else:
            reply('Вы ещё не зарегистрированы. Нажмите /start')
        return jsonify({'ok': True})

    # /shop
    if text == '/shop':
        partner = db.execute('SELECT name, ref_code FROM partners WHERE telegram_chat_id = ?', (chat_id,)).fetchone()
        if partner:
            reply(
                f'🏪 <b>Ваш магазин:</b>\n{APP_URL}/shop/{partner["ref_code"]}\n\n'
                f'Отправьте эту ссылку друзьям — заказы будут привязаны к вам!',
            )
        else:
            reply('Вы ещё не зарегистрированы. Нажмите /start')
        return jsonify({'ok': True})

    # /help
    if text == '/help':
        reply(
            '📋 <b>Команды бота:</b>\n\n'
            '/start — Открыть TULPAR\n'
            '/balance — Баланс и уровень\n'
            '/shop — Ваш магазин\n'
            '/help — Список команд'
        )
        return jsonify({'ok': True})

    return jsonify({'ok': True})


@app.get('/api/config')
def api_config():
    return jsonify({'botUsername': TG_BOT_USERNAME})


# ─── API: REVIEWS ────────────────────────────────────────────────────────

@app.get('/api/products/<int:pid>/reviews')
def api_get_reviews(pid):
    db = get_db()
    rows = [dict(r) for r in db.execute(
        'SELECT * FROM reviews WHERE product_id = ? ORDER BY created_at DESC', (pid,)
    ).fetchall()]
    avg = db.execute('SELECT AVG(rating) FROM reviews WHERE product_id = ?', (pid,)).fetchone()[0]
    count = len(rows)
    return jsonify({'reviews': rows, 'avg_rating': round(avg, 1) if avg else 0, 'count': count})


@app.post('/api/products/<int:pid>/reviews')
def api_add_review(pid):
    d = request.get_json(force=True) or {}
    name = (d.get('name') or '').strip()
    rating = int(d.get('rating') or 0)
    text = (d.get('text') or '').strip()
    if not name or rating < 1 or rating > 5:
        return jsonify({'error': 'Укажите имя и оценку от 1 до 5'}), 400
    db = get_db()
    db.execute(
        'INSERT INTO reviews (product_id, customer_name, rating, text) VALUES (?, ?, ?, ?)',
        (pid, name, rating, text),
    )
    db.commit()
    return jsonify({'success': True})


@app.get('/api/products/ratings')
def api_product_ratings():
    db = get_db()
    rows = db.execute(
        'SELECT product_id, AVG(rating) as avg_rating, COUNT(*) as count '
        'FROM reviews GROUP BY product_id'
    ).fetchall()
    return jsonify({str(r['product_id']): {'avg': round(r['avg_rating'], 1), 'count': r['count']} for r in rows})


# ─── API: REFERRAL TREE ─────────────────────────────────────────────────

@app.get('/api/partner/team-tree')
@auth_required
def api_team_tree():
    db = get_db()
    partner = db.execute('SELECT ref_code, name FROM partners WHERE id = ?', (request.partner_id,)).fetchone()
    if not partner:
        return jsonify({'tree': []})

    def get_children(ref_code, depth=0):
        if depth > 3:
            return []
        children = db.execute(
            'SELECT id, name, phone, city, tariff, ref_code, level, created_at '
            'FROM partners WHERE referrer = ? ORDER BY created_at DESC', (ref_code,)
        ).fetchall()
        result = []
        for c in children:
            c = dict(c)
            c['levelName'] = LEVEL_NAMES.get(c.get('level', 'member'), 'Участник')
            c['children'] = get_children(c['ref_code'], depth + 1)
            c['childCount'] = len(c['children'])
            result.append(c)
        return result

    tree = get_children(partner['ref_code'])
    total_l1 = len(tree)
    total_l2 = sum(len(c['children']) for c in tree)
    total_l3 = sum(sum(len(gc['children']) for gc in c['children']) for c in tree)

    return jsonify({
        'tree': tree,
        'stats': {'l1': total_l1, 'l2': total_l2, 'l3': total_l3, 'total': total_l1 + total_l2 + total_l3}
    })


# ─── API: PAYOUTS ────────────────────────────────────────────────────────

@app.post('/api/partner/request-payout')
@auth_required
def api_request_payout():
    db = get_db()
    partner = db.execute('SELECT id, name, balance FROM partners WHERE id = ?', (request.partner_id,)).fetchone()
    if not partner or partner['balance'] < 5000:
        return jsonify({'error': 'Минимальная сумма для вывода: 5,000 ₸'}), 400

    # Check no pending payouts
    pending = db.execute(
        "SELECT COUNT(*) FROM payouts WHERE partner_id = ? AND status = 'pending'",
        (request.partner_id,)
    ).fetchone()[0]
    if pending > 0:
        return jsonify({'error': 'У вас уже есть заявка на вывод в обработке'}), 400

    amount = partner['balance']
    db.execute(
        'INSERT INTO payouts (partner_id, amount, status) VALUES (?, ?, ?)',
        (request.partner_id, amount, 'pending'),
    )
    db.execute('UPDATE partners SET balance = 0 WHERE id = ?', (request.partner_id,))
    db.execute(
        'INSERT INTO transactions (partner_id, type, amount, description) VALUES (?, ?, ?, ?)',
        (request.partner_id, 'payout', -amount, f'Запрос на вывод {amount:,} ₸'),
    )
    db.commit()

    send_telegram(
        f'💸 <b>Запрос на вывод!</b>\n\n'
        f'👤 {partner["name"]}\n💰 Сумма: <b>{amount:,} ₸</b>\n'
        f'Подтвердите выплату в админке.'
    )

    return jsonify({'success': True, 'amount': amount, 'message': f'Заявка на {amount:,} ₸ отправлена!'})


@app.get('/api/partner/payouts')
@auth_required
def api_partner_payouts():
    db = get_db()
    rows = [dict(r) for r in db.execute(
        'SELECT * FROM payouts WHERE partner_id = ? ORDER BY created_at DESC', (request.partner_id,)
    ).fetchall()]
    return jsonify(rows)


@app.get('/api/admin/payouts')
@admin_required
def api_admin_payouts():
    db = get_db()
    rows = db.execute(
        'SELECT p.*, pr.name as partner_name, pr.phone as partner_phone '
        'FROM payouts p JOIN partners pr ON pr.id = p.partner_id '
        'ORDER BY p.created_at DESC'
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.patch('/api/admin/payouts/<int:pid>')
@admin_required
def api_update_payout(pid):
    d = request.get_json(force=True) or {}
    status = d.get('status', '')
    if status not in ('approved', 'rejected'):
        return jsonify({'error': 'Статус: approved или rejected'}), 400

    db = get_db()
    payout = db.execute('SELECT * FROM payouts WHERE id = ?', (pid,)).fetchone()
    if not payout:
        return jsonify({'error': 'Выплата не найдена'}), 404

    db.execute('UPDATE payouts SET status = ? WHERE id = ?', (status, pid))

    if status == 'rejected':
        # Return balance
        db.execute('UPDATE partners SET balance = balance + ? WHERE id = ?',
                   (payout['amount'], payout['partner_id']))
        db.execute(
            'INSERT INTO transactions (partner_id, type, amount, description) VALUES (?, ?, ?, ?)',
            (payout['partner_id'], 'payout_refund', payout['amount'], 'Возврат: заявка отклонена'),
        )

    db.commit()

    status_text = 'одобрена ✅' if status == 'approved' else 'отклонена ❌'
    send_partner_telegram(payout['partner_id'],
        f'💸 Ваша заявка на {payout["amount"]:,} ₸ {status_text}')

    return jsonify({'success': True})


# ─── API: DAILY CHECK-IN + COINS ─────────────────────────────────────────

CHECKIN_REWARDS = {1: 10, 2: 10, 3: 15, 4: 15, 5: 20, 6: 20, 7: 50}  # day → coins


@app.post('/api/partner/checkin')
@auth_required
def api_checkin():
    db = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Already checked in today?
    existing = db.execute(
        'SELECT * FROM checkins WHERE partner_id = ? AND date = ?',
        (request.partner_id, today),
    ).fetchone()
    if existing:
        return jsonify({'error': 'Вы уже отметились сегодня', 'already': True}), 400

    # Calculate streak
    partner = db.execute('SELECT last_checkin, checkin_streak FROM partners WHERE id = ?', (request.partner_id,)).fetchone()
    streak = 1
    if partner['last_checkin'] == yesterday:
        streak = (partner['checkin_streak'] % 7) + 1
    coins = CHECKIN_REWARDS.get(streak, 10)

    db.execute(
        'INSERT INTO checkins (partner_id, date, streak, coins_earned) VALUES (?, ?, ?, ?)',
        (request.partner_id, today, streak, coins),
    )
    db.execute(
        'UPDATE partners SET coins = coins + ?, last_checkin = ?, checkin_streak = ? WHERE id = ?',
        (coins, today, streak, request.partner_id),
    )
    db.commit()

    return jsonify({'success': True, 'coins': coins, 'streak': streak, 'message': f'+{coins} монет! Серия: {streak} дней'})


@app.get('/api/partner/checkin-status')
@auth_required
def api_checkin_status():
    db = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    partner = db.execute(
        'SELECT coins, last_checkin, checkin_streak FROM partners WHERE id = ?', (request.partner_id,)
    ).fetchone()
    checked_today = db.execute(
        'SELECT id FROM checkins WHERE partner_id = ? AND date = ?', (request.partner_id, today)
    ).fetchone() is not None
    return jsonify({
        'coins': partner['coins'],
        'streak': partner['checkin_streak'],
        'checkedToday': checked_today,
        'rewards': CHECKIN_REWARDS,
    })


# ─── API: COINS SPEND ───────────────────────────────────────────────────

@app.post('/api/orders/use-coins')
def api_use_coins_order():
    """Create order with coin discount. Coins sent in body."""
    d = request.get_json(force=True) or {}
    use_coins = int(d.get('use_coins') or 0)
    partner_ref = (d.get('partner_ref') or '').strip().upper()

    if use_coins > 0 and partner_ref:
        db = get_db()
        partner = db.execute('SELECT id, coins FROM partners WHERE ref_code = ?', (partner_ref,)).fetchone()
        if partner and partner['coins'] >= use_coins:
            db.execute('UPDATE partners SET coins = coins - ? WHERE id = ?', (use_coins, partner['id']))
            db.commit()

    # Delegate to normal order creation — coins are already deducted as discount
    return api_create_order()


# ─── API: SHARED CART ────────────────────────────────────────────────────

@app.post('/api/cart/share')
def api_share_cart():
    d = request.get_json(force=True) or {}
    items = d.get('items', [])
    partner_ref = (d.get('partner_ref') or '').strip()
    if not items:
        return jsonify({'error': 'Корзина пуста'}), 400
    code = secrets.token_hex(4)
    db = get_db()
    db.execute(
        'INSERT INTO shared_carts (code, items, partner_ref) VALUES (?, ?, ?)',
        (code, json_module.dumps(items), partner_ref),
    )
    db.commit()
    return jsonify({'success': True, 'code': code, 'url': f'/catalog?cart={code}'})


@app.get('/api/cart/<code>')
def api_get_shared_cart(code):
    db = get_db()
    row = db.execute('SELECT * FROM shared_carts WHERE code = ?', (code,)).fetchone()
    if not row:
        return jsonify({'error': 'Корзина не найдена'}), 404
    return jsonify({
        'items': json_module.loads(row['items']),
        'partner_ref': row['partner_ref'],
    })


# ─── API: REORDER ────────────────────────────────────────────────────────

@app.get('/api/partner/reorder')
@auth_required
def api_reorder():
    db = get_db()
    partner = db.execute('SELECT ref_code FROM partners WHERE id = ?', (request.partner_id,)).fetchone()
    if not partner:
        return jsonify([])
    # Get products from past orders by this partner's ref_code
    rows = db.execute('''
        SELECT DISTINCT p.* FROM products p
        JOIN order_items oi ON oi.product_id = p.id
        JOIN orders o ON o.id = oi.order_id
        WHERE o.partner_ref = ? AND p.in_stock = 1
        ORDER BY o.created_at DESC LIMIT 20
    ''', (partner['ref_code'],)).fetchall()
    return jsonify([dict(r) for r in rows])


# ─── HEALTH + KEEP ALIVE ─────────────────────────────────────────────────

@app.get('/health')
def health():
    return jsonify({'status': 'ok'})


def keep_alive():
    """Ping self every 10 minutes to prevent Render free tier from sleeping."""
    import threading
    import time as time_module
    def ping():
        while True:
            time_module.sleep(600)  # 10 minutes
            try:
                http_requests.get(f'https://tulpar-app.onrender.com/health', timeout=10)
            except Exception:
                pass
    t = threading.Thread(target=ping, daemon=True)
    t.start()


# Start keep-alive on import (works with gunicorn too)
keep_alive()


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
