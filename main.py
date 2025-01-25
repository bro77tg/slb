import os
import asyncio
import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils import executor
import aiosqlite
import ccxt
import time
import config
from concurrent.futures import ThreadPoolExecutor
import logging
import subprocess


# Команды для завершения работы и запуска скрипта в tmux
deactivate_command = config.deactivate_command
restore_command = config.restore_command

async def restore_api_keys():#Восстанавливает работу Passivebot
    """Запускает работу скрипта в tmux."""
    #Wait for 12 hours (in seconds)
    await asyncio.sleep(12 * 60 * 60)
    try:
        subprocess.run(restore_command, shell=True, check=True)
        print("Команда для запуска скрипта отправлена.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении команды: {e}")

async def deactivate_api_keys():#Завершает работу Passivebot
    """Завершает работу скрипта в tmux."""
    try:
        subprocess.run(deactivate_command, shell=True, check=True)
        print("Команда для завершения работы скрипта отправлена.")
        asyncio.create_task(restore_api_keys())
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении команды: {e}")



API_TOKEN = config.API_TOKEN

# Путь к файлу с API ключами
API_KEYS_DIR = config.API_KEYS_DIR
API_KEYS_FILE = config.API_KEYS_FILE
API_KEYS_DEACTIVATED_FILE = config.API_KEYS_DEACTIVATED_FILE

logging.basicConfig(level=logging.INFO)


# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())



async def setup_database():
    async with aiosqlite.connect('users.db') as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS user_keys (user_id INTEGER, exchange TEXT, key TEXT, secret TEXT, name TEXT, mode TEXT)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS user_limits (user_id INTEGER, key_name TEXT, limit_category TEXT, limit_type TEXT, limit_value REAL, PRIMARY KEY (user_id, key_name, limit_category, limit_type))''')
        await db.execute('''CREATE TABLE IF NOT EXISTS initial_balances (user_id INTEGER, key_name TEXT, initial_balance REAL, PRIMARY KEY (user_id, key_name))''')
        await db.execute('''CREATE TABLE IF NOT EXISTS daily_balances (user_id INTEGER NOT NULL, key_name TEXT NOT NULL, daily_balance REAL NOT NULL, date TEXT NOT NULL, PRIMARY KEY (user_id, key_name, date))''')
        await db.execute('''CREATE TABLE IF NOT EXISTS position_initial_balances (user_id INTEGER NOT NULL, key_name TEXT NOT NULL, initial_balance REAL NOT NULL, PRIMARY KEY (user_id, key_name))''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS max_drawdown (
                user_id INTEGER NOT NULL,
                key_name TEXT NOT NULL,
                max_drawdown REAL NOT NULL DEFAULT 0,
                date TEXT NOT NULL,
                PRIMARY KEY (user_id, key_name, date)
            );
        ''')
        await db.commit()


# States
class AddKeys(StatesGroup):
    exchange = State()
    key = State()
    secret = State()
    name = State()
    mode = State()

class SetLimits(StatesGroup):
    key_name = State()
    limit_target = State()
    limit_type = State()
    limit_value = State()

class ViewLimits(StatesGroup):
    key_name = State()

class CheckBalance(StatesGroup):
    key_name = State()
    
class RemoveKeys(StatesGroup):  # New state group for removing keys
    confirm = State()

# Helper function to send main menu
async def get_main_menu():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Добавить API ключи", "Удалить API ключи", "Установить лимиты", "Посмотреть лимиты", 
               "Баланс", "Дневник работы"]
    keyboard.add(*buttons)
    return keyboard

# Start command handler
@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.answer("Добро пожаловать в бота! Выберите действие:", reply_markup=await get_main_menu())

# Function to update initial balances in the database
async def update_initial_balances():
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT user_id, name FROM user_keys") as cursor:
            keys = await cursor.fetchall()
            for user_id, key_name in keys:
                async with db.execute("SELECT exchange, key, secret FROM user_keys WHERE user_id=? AND name=?", (user_id, key_name)) as key_cursor:
                    key_data = await key_cursor.fetchone()
                    if key_data:
                        exchange_name, api_key, api_secret = key_data
                        exchange = await create_exchange_instance(exchange_name, api_key, api_secret, is_testnet=False)
                        try:
                            balance = exchange.fetch_balance()
                            if exchange_name == "Bybit":
                                futures_balance = float(balance['info']['result']['list'][0]['coin'][0]['equity'])
                            elif exchange_name == "Binance":
                                futures_balance = float(balance['info']['totalMarginBalance'])
                                
                            # Суммируем НЕреализованный PnL всех открытых позиций
                            positions = exchange.fetch_positions() if hasattr(exchange, 'fetch_positions') else []
                            total_unrealized_pnl = sum(pos['unrealizedPnl'] for pos in positions if pos['contracts'] > 0 and 'unrealizedPnl' in pos)

                            # Обновляем начальный баланс для позиции, добавляя нереализованный PnL
                            new_initial_balance = futures_balance + -total_unrealized_pnl
                                
                                
                            await db.execute("""REPLACE INTO initial_balances (user_id, key_name, initial_balance)
                                                 VALUES (?, ?, ?)""", (user_id, key_name, new_initial_balance))
                        except Exception as e:
                            print(f"Error updating initial balance for {key_name}: {str(e)}")
        await db.commit()

async def update_position_initial_balances():
    """
    Обновляет начальный баланс в таблице position_initial_balances с учетом текущего баланса и PnL закрытых сделок.
    """
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT user_id, name FROM user_keys") as cursor:
            keys = await cursor.fetchall()
            for user_id, key_name in keys:
                async with db.execute("SELECT exchange, key, secret FROM user_keys WHERE user_id=? AND name=?", (user_id, key_name)) as key_cursor:
                    key_data = await key_cursor.fetchone()
                    if key_data:
                        exchange_name, api_key, api_secret = key_data
                        exchange = await create_exchange_instance(exchange_name, api_key, api_secret, is_testnet=False)
                        try:
                            balance = exchange.fetch_balance()
                            if exchange_name == "Bybit":
                                current_balance = float(balance['info']['result']['list'][0]['coin'][0]['equity'])
                            elif exchange_name == "Binance":
                                current_balance = float(balance['info']['totalMarginBalance'])


                            # # Суммируем НЕреализованный PnL всех открытых позиций
                            # positions = exchange.fetch_positions() if hasattr(exchange, 'fetch_positions') else []
                            # total_unrealized_pnl = sum(pos['unrealizedPnl'] for pos in positions if pos['contracts'] > 0 and 'unrealizedPnl' in pos)

                            # # Обновляем начальный баланс для позиции, добавляя нереализованный PnL
                            # new_initial_balance = current_balance + -total_unrealized_pnl

                            await db.execute("""REPLACE INTO position_initial_balances (user_id, key_name, initial_balance)
                                                 VALUES (?, ?, ?)""", (user_id, key_name, current_balance))
                        except Exception as e:
                            print(f"Error updating initial balance for {key_name}: {str(e)}")
        await db.commit()


# Функция для обновления дневного баланса в базе данных
async def update_daily_balances():
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT user_id, name FROM user_keys") as cursor:
            keys = await cursor.fetchall()
            for user_id, key_name in keys:
                async with db.execute("SELECT exchange, key, secret FROM user_keys WHERE user_id=? AND name=?", (user_id, key_name)) as key_cursor:
                    key_data = await key_cursor.fetchone()
                    if key_data:
                        exchange_name, api_key, api_secret = key_data
                        exchange = await create_exchange_instance(exchange_name, api_key, api_secret, is_testnet=False)
                        try:
                            balance = exchange.fetch_balance()
                            if exchange_name == "Bybit":
                                futures_balance = float(balance['info']['result']['list'][0]['coin'][0]['equity'])
                            elif exchange_name == "Binance":
                                futures_balance = float(balance['info']['totalMarginBalance'])
                            
                            # Запись начального дневного баланса в базу данных
                            today = datetime.datetime.now().date().strftime('%Y-%m-%d')
                            await db.execute("""REPLACE INTO daily_balances (user_id, key_name, daily_balance, date)
                                                 VALUES (?, ?, ?, ?)""", (user_id, key_name, futures_balance, today))
                        except Exception as e:
                            print(f"Error updating daily balance for {key_name}: {str(e)}")
        await db.commit()


# Schedule the update_initial_balances function to run daily at midnight
async def schedule_daily_update():
    while True:
        print("\n\n\n\n####################Updating initial balances...#######################\n\n\n\n")
        now = datetime.datetime.now()
        next_update = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        sleep_time = (next_update - now).total_seconds()
        await asyncio.sleep(sleep_time)
        await update_initial_balances()
        await update_position_initial_balances()
        await update_daily_balances()  # Обновление при запуске

async def schedule_daily_reset():
    """Планировщик для сброса максимальной просадки ежедневно."""
    while True:
        now = datetime.datetime.now()
        next_reset = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        sleep_time = (next_reset - now).total_seconds()
        await asyncio.sleep(sleep_time)
        await reset_max_drawdown()
        
        
# Add API keys
@dp.message_handler(lambda message: message.text == "Добавить API ключи")
async def add_keys_start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Bybit", "Binance", "Назад"]
    keyboard.add(*buttons)
    await AddKeys.exchange.set()
    await message.answer("Выберите биржу:", reply_markup=keyboard)

@dp.message_handler(lambda message: message.text not in ["Bybit", "Binance", "Назад"], state=AddKeys.exchange)
async def invalid_exchange(message: types.Message):
    await message.answer("Пожалуйста, выберите биржу из предложенных вариантов.")

@dp.message_handler(lambda message: message.text == "Назад", state=AddKeys)
async def cancel_add_keys(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Возвращение в главное меню.", reply_markup=await get_main_menu())

@dp.message_handler(state=AddKeys.exchange)
async def process_exchange(message: types.Message, state: FSMContext):
    await state.update_data(exchange=message.text)
    await AddKeys.next()
    await message.answer("Введите Key")

@dp.message_handler(state=AddKeys.key)
async def process_key(message: types.Message, state: FSMContext):
    await state.update_data(key=message.text)
    await AddKeys.next()
    await message.answer("Введите Secret")

@dp.message_handler(state=AddKeys.secret)
async def process_secret(message: types.Message, state: FSMContext):
    await state.update_data(secret=message.text)
    await AddKeys.next()
    await message.answer("Придумайте название")

@dp.message_handler(state=AddKeys.name)
async def process_name(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT 1 FROM user_keys WHERE user_id=? AND name=?", (message.from_user.id, message.text)) as cursor:
            if await cursor.fetchone():
                await message.answer("Ключ с таким названием уже существует. Пожалуйста, выберите другое название.")
                return

        await state.update_data(name=message.text)
        await AddKeys.next()
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        buttons = ["Hedge Mode", "No Hedge Mode"]
        keyboard.add(*buttons)
        await message.answer("Выберите режим аккаунта:", reply_markup=keyboard)
    
@dp.message_handler(state=AddKeys.mode)
async def process_mode(message: types.Message, state: FSMContext):
    if message.text not in ["Hedge Mode", "No Hedge Mode"]:
        await message.answer("Пожалуйста, выберите режим из предложенных вариантов.")
        return

    user_data = await state.get_data()
    mode = "hedge" if message.text == "Hedge Mode" else "no_hedge"

    async with aiosqlite.connect('users.db') as db:
        await db.execute("INSERT INTO user_keys (user_id, exchange, key, secret, name, mode) VALUES (?, ?, ?, ?, ?, ?)",
                         (message.from_user.id, user_data['exchange'], user_data['key'], user_data['secret'], user_data['name'], mode))
        await db.commit()
    # Update initial balance for the new key
    await update_initial_balances()
    await update_daily_balances()
    await update_position_initial_balances()
    await state.finish()
    await message.answer("Ваш ключ сохранён", reply_markup=await get_main_menu())




# Remove API keys and associated limits
@dp.message_handler(lambda message: message.text == "Удалить API ключи")
async def remove_keys_start(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT name FROM user_keys WHERE user_id=?", (user_id,)) as cursor:
            keys = await cursor.fetchall()
            if keys:
                keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
                for key in keys:
                    keyboard.add(key[0])
                keyboard.add("Назад")
                await RemoveKeys.confirm.set()  # Set the removal confirmation state
                await message.answer("Выберите ключ для удаления:", reply_markup=keyboard)
            else:
                await message.answer("У вас нет сохранённых ключей.", reply_markup=await get_main_menu())

@dp.message_handler(lambda message: message.text == "Назад")
async def go_back(message: types.Message):
    await message.answer("Возвращение в главное меню.", reply_markup=await get_main_menu())



@dp.message_handler(state=RemoveKeys.confirm) # Handler ONLY active in RemoveKeys.confirm state
async def remove_keys_confirm(message: types.Message, state: FSMContext):
    key_name = message.text
    user_id = message.from_user.id

    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT 1 FROM user_keys WHERE user_id=? AND name=?", (user_id, key_name)) as cursor:
            if await cursor.fetchone():  # Key exists and belongs to the user
                await db.execute("DELETE FROM user_keys WHERE user_id=? AND name=?", (user_id, key_name))
                await db.execute("DELETE FROM user_limits WHERE user_id=? AND key_name=?", (user_id, key_name))
                await db.execute("DELETE FROM initial_balances WHERE user_id=? AND key_name=?", (user_id, key_name))
                await db.execute("DELETE FROM position_initial_balances WHERE user_id=? AND key_name=?", (user_id, key_name))
                await db.execute("DELETE FROM position_initial_balances WHERE user_id=? AND key_name=?", (user_id, key_name))
                await db.execute("DELETE FROM max_drawdown WHERE user_id=? AND key_name=?", (user_id, key_name))
                await db.execute("DELETE FROM daily_balances WHERE user_id=? AND key_name=?", (user_id, key_name))
                await db.commit()
                await message.answer("Ваш ключ и связанные данные удалены", reply_markup=await get_main_menu())
            else:
                await message.answer("Ключ не найден или не принадлежит вам.", reply_markup=await get_main_menu())

    await state.finish() # Exit the removal confirmation state


# Set limits
@dp.message_handler(lambda message: message.text == "Установить лимиты")
async def set_limits_start(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT name FROM user_keys WHERE user_id=?", (user_id,)) as cursor:
            keys = await cursor.fetchall()
            if keys:
                keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
                for key in keys:
                    keyboard.add(key[0])
                keyboard.add("Назад")
                await SetLimits.key_name.set()
                await message.answer("Выберите ключ для установки лимита:", reply_markup=keyboard)
            else:
                await message.answer("У вас нет сохранённых ключей.", reply_markup=await get_main_menu())

@dp.message_handler(lambda message: message.text == "Назад", state=SetLimits)
async def cancel_limits(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Возвращение в главное меню.", reply_markup=await get_main_menu())

@dp.message_handler(state=SetLimits.key_name)
async def process_limit_key(message: types.Message, state: FSMContext):
    await state.update_data(key_name=message.text)
    await SetLimits.next()
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Позиция", "Баланс", "Назад"]
    keyboard.add(*buttons)
    await message.answer("Выберите для чего лимит:", reply_markup=keyboard)

@dp.message_handler(state=SetLimits.limit_target)
async def process_limit_target(message: types.Message, state: FSMContext):
    await state.update_data(limit_target=message.text)
    await SetLimits.next()
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Проценты", "В долларах", "Назад"]
    keyboard.add(*buttons)
    await message.answer("Выберите тип лимита:", reply_markup=keyboard)

@dp.message_handler(state=SetLimits.limit_type)
async def process_limit_type(message: types.Message, state: FSMContext):
    await state.update_data(limit_type=message.text)
    await SetLimits.next()
    await message.answer("Введите значение лимита:")

@dp.message_handler(state=SetLimits.limit_value)
async def process_limit_value(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    async with aiosqlite.connect('users.db') as db:
        await db.execute("""REPLACE INTO user_limits (user_id, key_name, limit_category, limit_type, limit_value)
                            VALUES (?, ?, ?, ?, ?)""",
                         (message.from_user.id, user_data['key_name'], user_data['limit_target'], user_data['limit_type'], float(message.text)))
        await db.commit()
    await state.finish()
    await message.answer("Лимит установлен", reply_markup=await get_main_menu())

# View limits
@dp.message_handler(lambda message: message.text == "Посмотреть лимиты")
async def view_limits_start(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT name FROM user_keys WHERE user_id=?", (user_id,)) as cursor:
            keys = await cursor.fetchall()
            if keys:
                keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
                for key in keys:
                    keyboard.add(key[0])
                keyboard.add("Назад")
                await ViewLimits.key_name.set()
                await message.answer("Выберите ключ для просмотра лимитов:", reply_markup=keyboard)
            else:
                await message.answer("У вас нет сохранённых ключей.", reply_markup= await get_main_menu())

@dp.message_handler(lambda message: message.text == "Назад", state=ViewLimits)
async def cancel_view_limits(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Возвращение в главное меню.", reply_markup= await get_main_menu())

@dp.message_handler(state=ViewLimits.key_name)
async def view_limits_for_key(message: types.Message, state: FSMContext):
    key_name = message.text
    user_id = message.from_user.id
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT exchange FROM user_keys WHERE user_id=? AND name=?", (user_id, key_name)) as cursor:
            exchange = await cursor.fetchone()
            if exchange:
                # Извлечем лимиты из базы данных
                async with db.execute("SELECT limit_category, limit_type, limit_value FROM user_limits WHERE user_id=? AND key_name=?", (user_id, key_name)) as limits_cursor:
                    limits = await limits_cursor.fetchall()
                    position_limits = []
                    balance_limits = []
                    
                    # Пройдем по всем лимитам и определим их категорию и тип
                    for limit in limits:
                        category, limit_type, value = limit
                        limit_str = f"{value} {limit_type}"
                        if category == "Позиция":
                            position_limits.append(limit_str)
                        elif category == "Баланс":
                            balance_limits.append(limit_str)

                    position_limits_str = ', '.join(position_limits) if position_limits else "Не установлен"
                    balance_limits_str = ', '.join(balance_limits) if balance_limits else "Не установлен"

                    await message.answer(
                        f"Биржа: {exchange[0]}\n"
                        f"API: {key_name}\n"
                        f"Лимит по позиции: {position_limits_str}\n"
                        f"Лимит по балансу: {balance_limits_str}",
                        reply_markup= await get_main_menu()
                    )
            else:
                await message.answer("Ключ не найден.", reply_markup= await get_main_menu())
    await state.finish()
    
# Check balance
@dp.message_handler(lambda message: message.text == "Баланс")
async def check_balance_start(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT name FROM user_keys WHERE user_id=?", (user_id,)) as cursor:
            keys = await cursor.fetchall()
            if keys:
                keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
                for key in keys:
                    keyboard.add(key[0])
                keyboard.add("Назад")
                await CheckBalance.key_name.set()
                await message.answer("Выберите ключ для проверки баланса:", reply_markup=keyboard)
            else:
                await message.answer("У вас нет сохранённых ключей.", reply_markup= await get_main_menu())

@dp.message_handler(lambda message: message.text == "Назад", state=CheckBalance)
async def cancel_check_balance(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Возвращение в главное меню.", reply_markup= await get_main_menu())


async def update_max_drawdown(user_id: int, key_name: str, current_balance: float):
    """Обновление максимальной просадки для пользователя и ключа."""
    today = datetime.datetime.now().date().strftime('%Y-%m-%d')
    async with aiosqlite.connect('users.db') as db:
        # Получение начального дневного баланса
        async with db.execute("SELECT daily_balance FROM daily_balances WHERE user_id=? AND key_name=? AND date=?", (user_id, key_name, today)) as cursor:
            daily_balance_data = await cursor.fetchone()
            daily_balance = daily_balance_data[0] if daily_balance_data else current_balance

        # Расчет текущей просадки
        current_drawdown = daily_balance - current_balance

        # Получение текущей максимальной просадки
        async with db.execute("SELECT max_drawdown FROM max_drawdown WHERE user_id=? AND key_name=? AND date=?", (user_id, key_name, today)) as cursor:
            max_drawdown_data = await cursor.fetchone()

        # Обновление максимальной просадки, если текущая просадка больше
        if max_drawdown_data:
            max_drawdown = max_drawdown_data[0]
            if current_drawdown > max_drawdown:
                await db.execute("UPDATE max_drawdown SET max_drawdown=? WHERE user_id=? AND key_name=? AND date=?", (current_drawdown, user_id, key_name, today))
        else:
            # Если запись отсутствует, добавляем новую
            await db.execute("INSERT INTO max_drawdown (user_id, key_name, max_drawdown, date) VALUES (?, ?, ?, ?)", (user_id, key_name, current_drawdown, today))

        await db.commit()

async def reset_max_drawdown():
    """Сброс максимальной просадки в начале каждого дня."""
    today = datetime.datetime.now().date().strftime('%Y-%m-%d')
    async with aiosqlite.connect('users.db') as db:
        await db.execute("DELETE FROM max_drawdown WHERE date!=?", (today,))
        await db.commit()

@dp.message_handler(state=CheckBalance.key_name)
async def check_balance_for_key(message: types.Message, state: FSMContext):
    key_name = message.text
    user_id = message.from_user.id
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT exchange, key, secret FROM user_keys WHERE user_id=? AND name=?", (user_id, key_name)) as cursor:
            key_data = await cursor.fetchone()
            if key_data:
                exchange_name, api_key, api_secret = key_data
                exchange = await create_exchange_instance(exchange_name, api_key, api_secret, is_testnet=False)
                
                try:
                    balance = exchange.fetch_balance()
                    positions = exchange.fetch_positions() if hasattr(exchange, 'fetch_positions') else []
                    open_orders = exchange.fetch_open_orders()
                    
                    # Calculate metrics
                    total_margin = sum(pos['initialMargin'] for pos in positions if 'initialMargin' in pos)
                    orders_value = sum(order['amount'] * order['price'] for order in open_orders)
                    
                    # Fetch initial balance and calculate PnL
                    async with db.execute("SELECT initial_balance FROM initial_balances WHERE user_id=? AND key_name=?", (user_id, key_name)) as balance_cursor:
                        initial_balance_data = await balance_cursor.fetchone()
                    initial_balance = initial_balance_data[0] if initial_balance_data else 0
                    if exchange_name == "Bybit":
                        current_balance = float(balance['info']['result']['list'][0]['coin'][0]['equity'])
                    elif exchange_name == "Binance":
                        current_balance = float(balance['info']['totalMarginBalance'])
                    pnl_24h = current_balance - initial_balance

                    # Обновляем максимальную просадку
                    await update_max_drawdown(user_id, key_name, current_balance)

                    # Получаем текущую максимальную просадку
                    today = datetime.datetime.now().date().strftime('%Y-%m-%d')
                    async with db.execute("SELECT max_drawdown FROM max_drawdown WHERE user_id=? AND key_name=? AND date=?", (user_id, key_name, today)) as drawdown_cursor:
                        max_drawdown_data = await drawdown_cursor.fetchone()
                    max_drawdown = max_drawdown_data[0] if max_drawdown_data else 0
                    async with db.execute("SELECT initial_balance FROM position_initial_balances WHERE user_id=? AND key_name=?", (user_id, key_name)) as pos_initbalance_cursor:
                        current_limit_balance_data = await pos_initbalance_cursor.fetchone()
                    current_limit_balance = current_limit_balance_data[0] if current_limit_balance_data else 0
                    
                    async with db.execute("SELECT initial_balance FROM initial_balances WHERE user_id=? AND key_name=?", (user_id, key_name)) as initbalance_cursor:
                        limit_balance_data = await initbalance_cursor.fetchone()
                    limit_balance = limit_balance_data[0] if limit_balance_data else 0
                    
                    
                    positions_list = [f"📌{pos['symbol']} {pos['contracts']} @ Entry price:{pos['entryPrice']} PnL: {pos['unrealizedPnl']}" for pos in positions if pos['contracts'] > 0]
                    orders_list = [f"🔁{order['symbol']} {order['amount']} @ {order['price']}" for order in open_orders]
                    
                    positions_str = ',\n '.join(positions_list) if positions_list else "Нет позиций"
                    orders_str = ',\n '.join(orders_list) if orders_list else "Нет ордеров"
                    await message.answer(
                        f"Биржа: {exchange_name}\n"
                        f"API name: {key_name}\n"
                        f"Баланс торговли 💲: {current_balance}\n"
                        f"Баланс для лимита позиции 💹: {current_limit_balance}\n"
                        f"Баланс для лимита по балансу 💵: {limit_balance}\n"
                        f"Прибыль/убыток после последнего обновления 🚀: {pnl_24h}\n"
                        f"Максимальная просадка за день 📉: {max_drawdown}\n"
                        f"Маржа позиций💰: {total_margin}\n"
                        f"Занято $ ордерами🔁: {orders_value}\n"
                        f"Позиции 📊:\n {positions_str}\n"
                        f"Ордера🎯: \n{orders_str}",
                        reply_markup= await get_main_menu()
                    )
                except Exception as e:
                    await message.answer(f"Ошибка получения данных: {str(e)}", reply_markup= await get_main_menu())
            else:
                await message.answer("Ключ не найден.", reply_markup= await get_main_menu())
    await state.finish()

# Обработчик для кнопки "Дневник работы"
@dp.message_handler(lambda message: message.text == "Дневник работы")
async def send_work_log(message: types.Message):
    user_id = message.from_user.id
    log_filename = f"user_{user_id}_log.txt"
    
    try:
        with open(log_filename, "r", encoding="utf-8", errors="ignore") as log_file:
            log_file.seek(0, 2)  # Перемещаем указатель в конец файла
            file_size = log_file.tell()  # Получаем размер файла
            start_position = max(0, file_size - 4000)  # Определяем позицию начала чтения
            log_file.seek(start_position)
            log_content = log_file.read()
        
        await message.answer(log_content if log_content else "Нет данных в дневнике.", reply_markup= await get_main_menu())
    except FileNotFoundError:
        await message.answer("Лог-файл не найден.", reply_markup= await get_main_menu())
    except Exception as e:
        await message.answer(f"Ошибка чтения лог-файла: {e}", reply_markup= await get_main_menu())



async def notify_user(user_id, message):
    """Отправить сообщение пользователю."""
    try:
        # c.execute("SELECT user_id FROM user_keys WHERE user_id=?", (user_id,))
        # chat_id = c.fetchone()
        chat_id = user_id
        if chat_id:
            await bot.send_message(chat_id=chat_id, text=message)
    except Exception as e:
        print(f"Ошибка отправки уведомления для пользователя {user_id}: {e}")

async def log_to_file(user_id, message):
    """Запись в лог-файл пользователя."""
    filename = f"user_{user_id}_log.txt"
    with open(filename, 'a', encoding='utf-8') as file:
        file.write(message + "\n")    
    
async def build_message(user_id, limit_type, closed_positions, closed_orders, total_loss, key_name):
    """Собрать детализированное сообщение для уведомления."""
    message = f"🔴 *Сработал лимит по {limit_type} для пользователя {user_id} и ключа {key_name}*\n\n"
    message += f"*Общая сумма убытков*: {total_loss}\n"
    
    message += "\nЗакрытые позиции:\n"
    for pos in closed_positions:
        message += f"• Символ: {pos['symbol']}, Контракты: {pos['contracts']}, PnL: {pos['pnl']}, Время закрытия: {pos['time']}\n"
    
    message += "\nОтмененные ордера:\n"
    for order in closed_orders:
        message += f"• Ордер ID: {order['id']}, Символ: {order['symbol']}, Время отмены: {order['time']}\n"
    
    return message



async def close_position_binance(exchange, symbol, position, mode):
    """
    Закрывает позицию на Binance в зависимости от режима (hedge или one-way).
    
    :param exchange: Экземпляр ccxt биржи Binance
    :param symbol: Торговая пара
    :param position: Словарь с данными о позиции
    :param mode: Режим торговли ('hedge' или 'no_hedge')
    """
    side = position['side']
    amount = abs(float(position['contracts']))

    params = {}
    if mode == 'hedge':
        # В режиме хеджирования необходимо указать позицию (длинная или короткая)
        params['positionSide'] = side.upper()

    try:
        order = exchange.create_order(
            symbol=symbol,
            side=('sell' if side == 'long' else 'buy'),
            type='MARKET',
            amount=amount,
            params=params
        )
        print(f"Closed BINANCE {side} position for {symbol}: {order}")
        return order
    except ccxt.ExchangeError as e:
        print(f"Error closing {side} position for {symbol}: {e}")
        return None

async def close_position_bybit(exchange, symbol, position, mode):
    """
    Закрывает позицию на Bybit в зависимости от режима (hedge или one-way).
    
    :param exchange: Экземпляр ccxt биржи Bybit
    :param symbol: Торговая пара
    :param position: Словарь с данными о позиции
    :param mode: Режим торговли ('hedge' или 'no_hedge')
    """
    side = position['side']
    amount = abs(float(position['contracts']))

    params = {}
    if mode == 'hedge':
        # В режиме хеджирования необходимо указать индекс позиции для Bybit
        params['position_idx'] = 1 if side == "long" else 2

    try:
        order = exchange.create_order(
            symbol=symbol,
            side=('sell' if side == 'long' else 'buy'),
            type='MARKET',
            amount=amount,
            params=params
        )
        print(f"Closed BYBIT {side} position for {symbol}: {order}")
        return order
    except ccxt.ExchangeError as e:
        print(f"Error closing {side} position for {symbol}: {e}")
        return None

async def check_balance_limit(user_id: int, key_name: str, current_balance: float) -> bool:
    """
    Проверяет, достигнут ли лимит по балансу.
    
    :param user_id: ID пользователя
    :param key_name: Название ключа
    :param current_balance: Текущий баланс
    :return: True, если лимит достигнут, иначе False
    """
    try:
        async with aiosqlite.connect('users.db') as db:
            # Получение первоначального баланса из базы данных
            async with db.execute("SELECT initial_balance FROM initial_balances WHERE user_id=? AND key_name=?", (user_id, key_name)) as cursor:
                initial_balance_data = await cursor.fetchone()
            initial_balance = initial_balance_data[0] if initial_balance_data else current_balance
            print("####Balance limit initial balance:", initial_balance)
            # Проверка лимита по процентам
            async with db.execute(
                "SELECT limit_value FROM user_limits WHERE user_id=? AND key_name=? AND limit_category='Баланс' AND limit_type='Проценты'",
                (user_id, key_name)
            ) as cursor:
                balance_limit_percent = await cursor.fetchone()
            if balance_limit_percent:
                print(f"balanlce limit percent: {balance_limit_percent[0]}")
            if balance_limit_percent and current_balance < initial_balance * (0.01 * balance_limit_percent[0]):
                print(f"Balance limit percent size: {initial_balance * (0.01 * balance_limit_percent[0])} < {current_balance}")
                return True

            # Проверка лимита в долларах
            async with db.execute(
                "SELECT limit_value FROM user_limits WHERE user_id=? AND key_name=? AND limit_category='Баланс' AND limit_type='В долларах'",
                (user_id, key_name)
            ) as cursor:
                balance_limit_dollars = await cursor.fetchone()
            if balance_limit_dollars:
                print(f"balanlce limit dollars: {balance_limit_dollars[0]}")
            if balance_limit_dollars and current_balance < initial_balance - balance_limit_dollars[0]:
                print(f"Balance limit dollars size: {initial_balance - balance_limit_dollars[0]} > {current_balance}")
                return True

            return False
    except Exception as e:
        print(f"Ошибка проверки лимита по балансу для {key_name}: {e}")
        return False

# async def restore_api_keys(original_path, deactivated_path):
#     """Wait for 12 hours and restore the original API keys file."""
#     # Wait for 12 hours (in seconds)
#     await asyncio.sleep(12 * 60 * 60)

#     # Restore the original file name
#     if os.path.exists(deactivated_path):
#         os.rename(deactivated_path, original_path)
#         logging.info(f"Renamed '{API_KEYS_DEACTIVATED_FILE}' back to '{API_KEYS_FILE}'.")
#     else:
#         logging.warning(f"Could not find '{API_KEYS_DEACTIVATED_FILE}' to restore.")



# async def deactivate_api_keys():    
#     """Rename the API keys file and start the restoration process."""
#     original_path = os.path.join(API_KEYS_DIR, API_KEYS_FILE)
#     deactivated_path = os.path.join(API_KEYS_DIR, API_KEYS_DEACTIVATED_FILE)

#     try:
#         # Rename the file if it exists
#         if os.path.exists(original_path):
#             os.rename(original_path, deactivated_path)
#             logging.info(f"Renamed '{API_KEYS_FILE}' to '{API_KEYS_DEACTIVATED_FILE}'.")

#             # Start the restoration process in the background
#             asyncio.create_task(restore_api_keys(original_path, deactivated_path))
#         else:
#             logging.warning(f"Original file '{API_KEYS_FILE}' does not exist.")

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
        
        
async def create_exchange_instance(exchange_name: str, api_key: str, api_secret: str, is_testnet: bool = False) -> ccxt.Exchange:
    """Создает экземпляр биржи на основе названия биржи и API ключей."""
    if exchange_name.lower() == "binance":
        exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'warnOnFetchOpenOrdersWithoutSymbol': False,
            },
        })
        if is_testnet:
            exchange.set_sandbox_mode(True)  # Активирует тестовый режим
        return exchange

    elif exchange_name.lower() == "bybit":
        exchange = ccxt.bybit({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},
        })
        if is_testnet:
            exchange.set_sandbox_mode(True)  # Активирует тестовый режим
        return exchange

    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

async def check_and_close_positions():
    # print("CALLED check_and_close_positions()")
    async with aiosqlite.connect('users.db') as db:
        async with db.execute("SELECT user_id, name, exchange, key, secret, mode FROM user_keys") as cursor:
            keys = await cursor.fetchall()
            # print("Fetched keys:", keys)
            # Получение первоначального баланса из базы данных

            for user_id, key_name, exchange_name, api_key, api_secret, mode in keys:
                
                async with db.execute("SELECT initial_balance FROM position_initial_balances WHERE user_id=? AND key_name=?", (user_id, key_name)) as cursor:
                    initial_balance_data = await cursor.fetchone()
                initial_pos_balance = initial_balance_data[0] if initial_balance_data else 0  # Если None, то 0
                print(f"initial_pos_balance for {key_name}: {initial_pos_balance}")
                
                # print(f"Processing key {key_name} for user {user_id}")
                # Инициализация биржи
                exchange = await create_exchange_instance(exchange_name, api_key, api_secret, is_testnet=False)
                # print(f"Initialized exchange for {exchange_name}")
                
                try:
                    balance = exchange.fetch_balance()
                    print(f"Fetched balance: {balance}")
                    #Bybit
                    # print(f"totalPerpUPL: {balance['info']['result']['list'][0]['totalPerpUPL']}")
                    #Binance
                    # print(f"totalPerpUPL: {balance['info']['totalUnrealizedProfit']}")
                    # print(f"Fetched balance: totalEquity: {balance['info']['result']['list'][0]['totalEquity']} totalAvailableBalance: {balance['info']['result']['list'][0]['totalAvailableBalance']} equity: --- ")
                    positions = exchange.fetch_positions() if hasattr(exchange, 'fetch_positions') else []
                    print(f"Fetched positions: {positions}")
                    closed_positions = []
                    closed_orders = []
                    total_loss = 0
                    
                    # Проверка лимита по позиции
                    for pos in positions:
                        symbol = pos['symbol']
                        print(f"Fetched pos symb: {symbol}")
                        unrealized_pnl = pos['unrealizedPnl']
                        contracts = pos['contracts']
                        entry_price = pos['entryPrice']
                        side = pos['side']

                        
                        # Проверка, что все необходимые данные присутствуют
                        if contracts is None or entry_price is None or contracts == 0:
                            print(f"Skipping position for {symbol} due to missing data")
                            continue  # Пропуск итерации, если данные отсутствуют или некорректны
                        
                        position_limit_triggered = False

                        # Лимит по позиции в процентах
                        async with db.execute(
                            "SELECT limit_value FROM user_limits WHERE user_id=? AND key_name=? AND limit_category='Позиция' AND limit_type='Проценты'",
                            (user_id, key_name)
                        ) as cursor:
                            limit_percent = await cursor.fetchone()
                        if limit_percent:
                            print(f"Limit precent: {limit_percent}")
                            if limit_percent and unrealized_pnl < initial_pos_balance * (0.01* limit_percent[0]):
                                position_limit_triggered = True
                                print(f"For {key_name} @ {symbol} position limit triggered in percent")

                        # Лимит по позиции в долларах
                        async with db.execute(
                            "SELECT limit_value FROM user_limits WHERE user_id=? AND key_name=? AND limit_category='Позиция' AND limit_type='В долларах'",
                            (user_id, key_name)
                        ) as cursor:
                            limit_dollars = await cursor.fetchone()
                        print(f"Unrealized PNL for {key_name} @ {symbol}: {unrealized_pnl}")
                        if limit_dollars:
                            print(f"limit dollars: {-limit_dollars[0]} -- {unrealized_pnl}")
                            if limit_dollars and unrealized_pnl < -limit_dollars[0]:
                                position_limit_triggered = True
                                print(f"For {key_name} @ {symbol} position limit triggered in dollars")
                        
                        # Закрытие позиций при срабатывании лимита ПОЗИЦИЙ
                        if position_limit_triggered:
                            await close_position_and_orders(exchange, symbol, positions, closed_positions, closed_orders, mode)
                            total_loss += unrealized_pnl
                            await update_position_initial_balances()
                            break  # Прерывание цикла, чтобы избежать повторного закрытия для того же символа

                    # Проверка лимита по балансу
                    current_balance = float(balance['info']['totalMarginBalance']) if exchange_name.lower() == "binance" else float(balance['info']['result']['list'][0]['coin'][0]['equity'])
                    balance_limit_triggered = await check_balance_limit(user_id, key_name, current_balance)

                    if balance_limit_triggered:
                        closed_positions, closed_orders, total_loss = await close_all_positions_and_orders(keys)
                        await deactivate_api_keys()  # Деактивация API ключей
                        await update_position_initial_balances()
                        # await update_initial_balances()
                        
                    if closed_positions or closed_orders:
                        print("NOTIFY CALLED")
                        message = await build_message(user_id, "балансу" if balance_limit_triggered else "позиции", closed_positions, closed_orders, total_loss, key_name)
                        await notify_user(user_id, message)
                        await log_to_file(user_id, message)  
                
                except ccxt.ExchangeError as e:
                    print(f"Ошибка API для {exchange_name}: {e}")
                except Exception as e:
                    print(f"Ошибка проверки лимитов для {exchange_name}: {e}")

async def close_position_and_orders(exchange, symbol, positions, closed_positions, closed_orders, mode):
    """Закрытие всех позиций и ордеров с указанным символом."""
    try:
        for position in positions:
            if position['symbol'] == symbol:
                # Закрытие позиции
                if exchange.id == "binance":
                    await close_position_binance(exchange, symbol, position, mode)
                elif exchange.id == "bybit":
                    await close_position_bybit(exchange, symbol, position, mode)

                # Логирование закрытой позиции
                closed_positions.append({
                    'symbol': symbol,
                    'contracts': position['contracts'],
                    'pnl': position['unrealizedPnl'],
                    'time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

        # Отмена всех открытых ордеров для данного символа
        for order in exchange.fetch_open_orders(symbol=symbol):
            # exchange.cancel_order(order['id'], symbol=symbol)
            exchange.cancel_all_orders(symbol=symbol)
            closed_orders.append({
                'id': order['id'],
                'symbol': symbol,
                'time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

    except Exception as e:
        print(f"Ошибка закрытия позиций и ордеров для {symbol}: {e}")

async def close_all_positions_and_orders(keys):
    """Закрыть все позиции и ордера на всех биржах для всех API-ключей."""
    closed_positions = []
    closed_orders = []
    total_loss = 0

    for user_id, key_name, exchange_name, api_key, api_secret, mode in keys:
        try:
            exchange = await create_exchange_instance(exchange_name, api_key, api_secret, is_testnet=False)
            
            # Закрытие всех позиций
            positions = exchange.fetch_positions() if hasattr(exchange, 'fetch_positions') else []
            for position in positions:
                symbol = position.get('symbol', None)
                unrealized_pnl = position.get('unrealizedPnl', 0)
                if symbol:
                    if exchange.id == "binance":
                        await close_position_binance(exchange, symbol, position, mode)
                    elif exchange.id == "bybit":
                        await close_position_bybit(exchange, symbol, position, mode)

                    # Логирование закрытой позиции
                    closed_positions.append({
                        'symbol': symbol,
                        'contracts': position.get('contracts', 0),
                        'pnl': unrealized_pnl,
                        'time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    total_loss += unrealized_pnl
                    print(f"Позиция по {symbol} закрыта для {key_name}")

            # Отмена всех ордеров
            open_orders = exchange.fetch_open_orders()
            for order in open_orders:
                symbol = order.get('symbol', None)
                if symbol and 'id' in order:
                    exchange.cancel_all_orders(symbol=symbol)
                    closed_orders.append({
                        'id': order['id'],
                        'symbol': symbol,
                        'time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    print(f"Ордер {order['id']} по {symbol} отменен для {key_name}")
            # for order in open_orders:
            #     symbol = order.get('symbol', None)
            #     if symbol and 'id' in order:
            #         exchange.cancel_order(order['id'], symbol=symbol)
            #         closed_orders.append({
            #             'id': order['id'],
            #             'symbol': symbol,
            #             'time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #         })
            #         print(f"Ордер {order['id']} по {symbol} отменен для {key_name}")

        except Exception as e:
            print(f"Ошибка закрытия позиций и ордеров для {exchange_name}: {e}")

    return closed_positions, closed_orders, total_loss



   
            
async def schedule_limit_checks():
    while True:
        await check_and_close_positions()
        # await asyncio.sleep(0.1)  # 1 sec limit check time 



    
async def main():
    await setup_database()
    original_path = os.path.join(API_KEYS_DIR, API_KEYS_FILE)
    deactivated_path = os.path.join(API_KEYS_DIR, API_KEYS_DEACTIVATED_FILE)

    # Возвращение исходного имени файла
    if os.path.exists(deactivated_path):
        os.rename(deactivated_path, original_path)
        print(f"Файл {API_KEYS_DEACTIVATED_FILE} переименован обратно в {API_KEYS_FILE}")
    
    
    # NO loop creation here! asyncio.run handles it.
    await update_initial_balances() # Await the initial balance update
    asyncio.create_task(schedule_daily_update())
    
    
    asyncio.create_task(schedule_limit_checks())
    
    await update_daily_balances()    # Обновление дневных балансов при старте
    await update_position_initial_balances()
    await reset_max_drawdown() # Обновление макс просадки при старте
    asyncio.create_task(schedule_daily_reset())  # Добавляем планировщик сброса просадки
    
    try:
        print("Bot started. PassiveBot Started.")
        subprocess.run(restore_command, shell=True, check=True)
        await dp.start_polling()
    except (KeyboardInterrupt, SystemExit):
        print("Exсept.")
    finally:
        print("Bot stopped. PassiveBot Stopped")
        subprocess.run(deactivate_command, shell=True, check=True)
        await dp.stop_polling()
        await bot.close()

if __name__ == '__main__':
    asyncio.run(main())  # Use asyncio.run for proper shutdown handling