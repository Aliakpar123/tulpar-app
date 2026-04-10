# 🦅 TULPAR — Социальная коммерция

Платформа партнёрских продаж, вдохновлённая китайским Yunji.

---

## 🚀 Быстрый старт

### Требования
- Python 3.8+

### 1. Установка и запуск

**Linux / macOS:**
```bash
chmod +x start.sh
./start.sh
```

**Windows:**
```
Двойной клик на start.bat
```

**Вручную:**
```bash
pip install -r requirements.txt
python3 server.py
```

### 2. Откройте в браузере
```
http://localhost:3000
```

---

## ⚙️ Настройка Telegram уведомлений

1. Найдите `@BotFather` в Telegram
2. Отправьте `/newbot` и следуйте инструкции
3. Скопируйте токен бота
4. Откройте `@userinfobot` и скопируйте свой Chat ID
5. Отредактируйте файл `.env`:
   ```
   TELEGRAM_BOT_TOKEN=ваш_токен_здесь
   TELEGRAM_ADMIN_CHAT_ID=ваш_chat_id
   ```
6. Перезапустите сервер

---

## 🔐 Доступы

| Роль | URL | Данные для входа |
|------|-----|-----------------|
| Главная | http://localhost:3000 | — |
| Регистрация | http://localhost:3000/register | — |
| Кабинет партнёра | http://localhost:3000/login | Номер телефона |
| Админ-панель | http://localhost:3000/admin | Пароль из .env |

**Пароль администратора по умолчанию:** `tulpar2024`
(Измените в `.env` → поле `ADMIN_PASSWORD`)

---

## 💼 Тарифы

| Тариф | Взнос | Комиссия с личных продаж |
|-------|-------|--------------------------|
| СТАРТ | 15,000 ₸ | 8% |
| ПАРТНЁР | 50,000 ₸ | 12% |
| ЛИДЕР | 150,000 ₸ | 15% |

Командные бонусы: 3% (уровень 1), 1% (уровень 2)

---

## 📁 Структура проекта

```
tulpar_app/
├── server.py          # Основной сервер (Flask)
├── requirements.txt   # Python-зависимости
├── .env               # Конфигурация (токены, пароли)
├── tulpar.db          # База данных SQLite (создаётся автоматически)
├── start.sh           # Скрипт запуска Linux/macOS
├── start.bat          # Скрипт запуска Windows
└── public/            # Фронтенд файлы
    ├── index.html     # Лендинг
    ├── register.html  # Форма регистрации
    ├── login.html     # Страница входа
    ├── dashboard.html # Кабинет партнёра
    └── admin.html     # Админ-панель
```

---

## 🌐 API Endpoints

| Метод | URL | Описание |
|-------|-----|----------|
| POST | /api/register | Регистрация партнёра |
| POST | /api/login | Вход партнёра |
| POST | /api/admin/login | Вход администратора |
| GET | /api/partner/me | Профиль партнёра |
| GET | /api/admin/stats | Статистика (admin) |
| GET | /api/admin/partners | Список партнёров (admin) |
| PATCH | /api/admin/partner/:id | Обновить партнёра (admin) |
| DELETE | /api/admin/partner/:id | Удалить партнёра (admin) |
| GET | /api/admin/export | Экспорт CSV (admin) |

---

## 🔧 Деплой на сервер (VPS)

```bash
# Установка на Ubuntu/Debian
sudo apt install python3 python3-pip nginx -y
pip3 install flask flask-cors gunicorn

# Запуск через gunicorn (production)
gunicorn -w 4 -b 0.0.0.0:3000 server:app

# Или через systemd service для автозапуска
```

---

*TULPAR — Тұлпар — мифический крылатый конь казахской мифологии*
