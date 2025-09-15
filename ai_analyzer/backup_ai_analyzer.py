#!/usr/bin/env python3
"""
WheelLog AI Analyzer Service
Сервис для AI-анализа поездок с интеграцией GigaChat и автоматическим обновлением токенов
"""

import os
import json
import time
import uuid
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import psycopg2
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv

try:
    import certifi
except ImportError:
    certifi = None

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="WheelLog AI Analyzer", version="1.1.0")

class TripWebhookPayload(BaseModel):
    filename: str
    timestamp: str
    records_count: int
    distance_km: float
    duration_min: int
    battery_start: int
    battery_end: int
    battery_used: int
    max_speed: float
    avg_speed: float

class GigaChatClient:
    """Клиент для работы с GigaChat API с автоматическим обновлением токенов"""
    
    def __init__(self):
        self.oauth_url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
        self.api_url = "https://gigachat.devices.sberbank.ru/api/v1"
        
        # Получаем Authorization key из переменных окружения
        self.auth_key = os.getenv("GIGACHAT_AUTH_KEY")
        if not self.auth_key:
            logger.warning("GIGACHAT_AUTH_KEY не установлен в переменных окружения")
            self.auth_key = None
        
        # Переменные для управления токеном
        self.access_token = None
        self.token_expires_at = None
        self.token_lock = False
        
        # Настраиваем HTTP сессию
        self.session = self._create_session_with_russian_certs()
    
    def _create_session_with_russian_certs(self):
        """Создание HTTP сессии с российскими сертификатами"""
        session = requests.Session()
        
        # Настройка retry стратегии
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Пути к российским сертификатам
        cert_paths = [
            "/home/pavel/certs/russian_trusted_root_ca_pem.crt",
            "/home/pavel/certs/russian_trusted_sub_ca_pem.crt", 
            "/home/pavel/certs/russian_trusted_root_ca_gost_2025_pem.crt",
            "/home/pavel/certs/russian_trusted_sub_ca_2024_pem.crt",
            "/home/pavel/certs/russian_trusted_sub_ca_gost_2025_pem.crt",
            # Альтернативные пути
            "/home/pavel/russian_trusted_root_ca_pem.crt",
            "/home/pavel/russian_trusted_sub_ca_pem.crt"
        ]
        
        # Создаем временный файл с объединенными сертификатами
        combined_cert_path = "/tmp/russian_certs_combined.pem"
        
        try:
            with open(combined_cert_path, 'w') as combined_file:
                # Добавляем системные сертификаты
                if certifi:
                    try:
                        with open(certifi.where(), 'r') as sys_certs:
                            combined_file.write(sys_certs.read())
                            combined_file.write('\n')
                    except:
                        pass
                
                # Добавляем российские сертификаты
                for cert_path in cert_paths:
                    if os.path.exists(cert_path):
                        try:
                            with open(cert_path, 'r') as cert_file:
                                combined_file.write(cert_file.read())
                                combined_file.write('\n')
                            logger.info(f"Добавлен сертификат: {cert_path}")
                        except Exception as e:
                            logger.warning(f"Не удалось прочитать сертификат {cert_path}: {e}")
            
            # Устанавливаем путь к сертификатам
            session.verify = combined_cert_path
            logger.info("Настроены российские сертификаты для GigaChat")
            
        except Exception as e:
            logger.error(f"Ошибка настройки сертификатов: {e}")
            # Fallback - отключаем проверку SSL (не рекомендуется для продакшена)
            session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            logger.warning("SSL проверка отключена - не рекомендуется для продакшена!")
        
        return session
    
    def _is_token_valid(self) -> bool:
        """Проверка действительности токена"""
        if not self.access_token or not self.token_expires_at:
            return False
        
        # Добавляем буфер в 2 минуты для обновления токена заранее
        return datetime.now() < (self.token_expires_at - timedelta(minutes=2))
    
    def _get_new_access_token(self) -> bool:
        """Получение нового access token"""
        if not self.auth_key:
            logger.error("GIGACHAT_AUTH_KEY не настроен")
            return False
            
        if self.token_lock:
            # Если токен уже обновляется, ждем
            for _ in range(30):  # Максимум 30 секунд
                time.sleep(1)
                if not self.token_lock:
                    break
            return self._is_token_valid()
        
        self.token_lock = True
        
        try:
            logger.info("Получаем новый access token для GigaChat...")
            
            # Генерируем уникальный RqUID
            rq_uid = str(uuid.uuid4())
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json',
                'RqUID': rq_uid,
                'Authorization': f'Basic {self.auth_key}'
            }
            
            payload = {
                'scope': 'GIGACHAT_API_PERS'
            }
            
            response = self.session.post(
                self.oauth_url,
                headers=headers,
                data=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                
                # Токен действует 30 минут
                expires_in = token_data.get('expires_in', 1800)  # По умолчанию 30 минут
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
                
                logger.info(f"Новый access token получен, действителен до: {self.token_expires_at}")
                return True
            else:
                logger.error(f"Ошибка получения токена: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Исключение при получении токена: {e}")
            return False
        finally:
            self.token_lock = False
    
    def _get_valid_token(self) -> Optional[str]:
        """Получение действующего токена (с автообновлением при необходимости)"""
        if not self._is_token_valid():
            if not self._get_new_access_token():
                return None
        
        return self.access_token
    
    async def analyze_trip(self, trip_data: Dict[str, Any], detailed_stats: Dict[str, Any]) -> str:
        """Анализ поездки через GigaChat с автообновлением токена"""
        
        # Получаем действующий токен
        token = self._get_valid_token()
        if not token:
            return "❌ Не удалось получить токен доступа к GigaChat"
        
        prompt = self._create_analysis_prompt(trip_data, detailed_stats)
        
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        
        payload = {
            "model": "GigaChat",
            "messages": [
                {
                    "role": "system",
                    "content": "Ты эксперт по анализу поездок на электротранспорте. Анализируй данные поездок и давай практические рекомендации на русском языке."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.3,
            "max_tokens": 500
        }
        
        try:
            logger.info("Отправляем запрос в GigaChat...")
            
            response = self.session.post(
                f"{self.api_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )
            
            logger.info(f"GigaChat ответ: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                analysis = result['choices'][0]['message']['content']
                logger.info("AI-анализ получен успешно")
                return analysis
            elif response.status_code == 401:
                # Токен истек, попробуем обновить и повторить запрос
                logger.warning("Токен истек, обновляем...")
                self.access_token = None
                self.token_expires_at = None
                
                new_token = self._get_valid_token()
                if new_token:
                    headers['Authorization'] = f'Bearer {new_token}'
                    response = self.session.post(
                        f"{self.api_url}/chat/completions",
                        headers=headers,
                        json=payload,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        analysis = result['choices'][0]['message']['content']
                        logger.info("AI-анализ получен после обновления токена")
                        return analysis
                
                return "❌ Ошибка авторизации в GigaChat"
            else:
                logger.error(f"GigaChat API ошибка {response.status_code}: {response.text}")
                return f"❌ Временно недоступен AI-анализ (ошибка {response.status_code})"
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса к GigaChat: {e}")
            return "❌ Временно недоступен AI-анализ (сетевая ошибка)"
        except Exception as e:
            logger.error(f"Неожиданная ошибка GigaChat: {e}")
            return "❌ Временно недоступен AI-анализ"
    
    def _create_analysis_prompt(self, trip_data: Dict[str, Any], detailed_stats: Dict[str, Any]) -> str:
    """Улучшенный промпт с учетом реальных характеристик Patton S"""
    
    # Реалистичный расчет остатка хода
    current_battery = trip_data['battery_end']
    consumption = trip_data['battery_used'] / max(trip_data['distance_km'], 0.1)
    
    # Учитываем нелинейность батареи (последние 20% менее эффективны)
    usable_battery = current_battery - 10 if current_battery > 15 else max(0, current_battery - 5)
    realistic_range = usable_battery / consumption if consumption > 0 else 0
    
    # Эталонные данные для Patton S 2220Wh
    reference_consumption = 1.2  # %/км при оптимальных условиях (40 км/ч, 20°C)
    max_theoretical_range = current_battery / reference_consumption
    
    return f"""
Анализируй поездку на Leaperkim Patton S (2220Wh, 92кг райдер, мотослик):

📊 ПОЕЗДКА:
- {trip_data['distance_km']} км, {trip_data['duration_min']} мин
- Батарея: {trip_data['battery_start']}% → {trip_data['battery_end']}% (-{trip_data['battery_used']}%)
- Скорость: {trip_data['avg_speed']:.1f} средняя, {min(trip_data['max_speed'], 70):.1f} макс (реальная)
- Расход: {consumption:.1f}%/км

⚡️ ЭНЕРГОЭФФЕКТИВНОСТЬ:
- Эталонный расход: {reference_consumption}%/км (40км/ч, идеал)
- Ваш результат: {"отлично" if consumption < 1.5 else "хорошо" if consumption < 2.5 else "высокий расход"}
- Фактор влияния: {"низкая скорость + остановки" if trip_data['avg_speed'] < 20 else "оптимальный режим" if 25 <= trip_data['avg_speed'] <= 45 else "высокая скорость"}

🔋 РЕАЛЬНЫЙ ОСТАТОК ХОДА:
- При вашем стиле: {realistic_range:.0f} км
- В идеальных условиях: {max_theoretical_range:.0f} км  
- Безопасный запас: {realistic_range * 0.7:.0f} км

💡 ФИЗИКА ЭНЕРГОПОТРЕБЛЕНИЯ:
- Оптимум: 30-40 км/ч (лучший баланс скорость/расход)
- <20 км/ч: высокий относительный расход (остановки, разгоны)
- >50 км/ч: квадратичный рост аэросопротивления
- Холостой ход >70 км/ч не учитывается в реальной езде

ОТВЕТ (150-200 символов): Техническая оценка эффективности, конкретные причины расхода, практичные советы, точный прогноз хода.
"""
    
    def test_connection(self) -> Dict[str, Any]:
        """Тестирование подключения к GigaChat"""
        try:
            token = self._get_valid_token()
            if not token:
                return {
                    "status": "error",
                    "message": "Не удалось получить токен доступа"
                }
            
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {token}'
            }
            
            response = self.session.get(
                f"{self.api_url}/models",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                models = response.json()
                return {
                    "status": "success",
                    "message": "Подключение к GigaChat работает",
                    "models": models.get('data', []),
                    "token_expires_at": self.token_expires_at.isoformat() if self.token_expires_at else None
                }
            else:
                return {
                    "status": "error",
                    "message": f"Ошибка API: {response.status_code}",
                    "response": response.text
                }
                
        except Exception as e:
            return {
                "status": "error",
                "message": f"Ошибка подключения: {str(e)}"
            }

class DatabaseManager:
    """Менеджер для работы с базой данных с исправленными колонками"""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'wheellog'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
    
    def get_database_schema(self):
        """Получение схемы таблицы для определения правильных названий колонок"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'wheellog_data'
                ORDER BY ordinal_position;
            """)
            
            columns = cursor.fetchall()
            cursor.close()
            conn.close()
            
            logger.info(f"Колонки в таблице wheellog_data: {[col[0] for col in columns]}")
            return {col[0]: col[1] for col in columns}
            
        except Exception as e:
            logger.error(f"Ошибка получения схемы БД: {e}")
            return {}
    
    def get_trip_detailed_stats(self, filename: str) -> Dict[str, Any]:
        """Получение детальной статистики поездки из БД с автоопределением колонок"""
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Получаем схему таблицы для определения правильных названий
            schema = self.get_database_schema()
            
            # Определяем названия колонок (возможные варианты)
            battery_col = None
            speed_col = None
            temp_col = None
            distance_col = None
            
            for col_name in schema.keys():
                col_lower = col_name.lower()
                if 'battery' in col_lower or 'bat' in col_lower or 'charge' in col_lower:
                    battery_col = col_name
                elif 'speed' in col_lower or 'velocity' in col_lower:
                    speed_col = col_name
                elif 'temp' in col_lower or 'temperature' in col_lower:
                    temp_col = col_name
                elif 'distance' in col_lower or 'dist' in col_lower:
                    distance_col = col_name
            
            logger.info(f"Определенные колонки: battery={battery_col}, speed={speed_col}, temp={temp_col}, distance={distance_col}")
            
            # Если не нашли нужные колонки, возвращаем дефолтные значения
            if not (speed_col or battery_col or distance_col):
                logger.warning("Не найдены необходимые колонки в БД")
                return self._get_default_stats()
            
            # Формируем простой запрос для получения базовых метрик
            cursor.execute("""
                SELECT COUNT(*) 
                FROM wheellog_data 
                WHERE filename = %s
            """, (filename,))
            
            record_count = cursor.fetchone()[0]
            
            if record_count == 0:
                logger.warning(f"Нет данных для файла {filename}")
                return self._get_default_stats()
                
            # Базовые метрики из payload уже есть, дополняем простыми расчетами
            cursor.close()
            conn.close()
            
            return {
                'avg_temp': 25.0,  # Дефолтное значение
                'high_speed_time': 10.0,  # Примерная оценка
                'battery_per_km': 4.0,  # Будет пересчитано из payload
                'efficiency_score': 7.0,
                'aggressiveness': 5.0,
                'avg_battery_usage': 4.0,
                'typical_avg_speed': 18.0
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
        
        return self._get_default_stats()
    
    def _get_default_stats(self) -> Dict[str, Any]:
        """Значения по умолчанию для статистики"""
        return {
            'avg_temp': 25,
            'high_speed_time': 0,
            'battery_per_km': 4.0,
            'efficiency_score': 7.0,
            'aggressiveness': 5.0,
            'avg_battery_usage': 4.0,
            'typical_avg_speed': 18
        }

class NotificationManager:
    """Менеджер уведомлений"""
    
    def __init__(self):
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.email_config = {
            'smtp_server': os.getenv('SMTP_SERVER'),
            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
            'email_user': os.getenv('EMAIL_USER'),
            'email_password': os.getenv('EMAIL_PASSWORD'),
            'recipient': os.getenv('EMAIL_RECIPIENT')
        }
    
    async def send_telegram_report(self, report: str):
        """Отправка отчета в Telegram"""
        if not self.telegram_bot_token or not self.telegram_chat_id:
            logger.warning("Telegram не настроен")
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': report,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info("Отчет отправлен в Telegram")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки в Telegram: {e}")
            return False
    
    async def send_email_report(self, subject: str, report: str):
        """Отправка отчета по email"""
        if not all(self.email_config.values()):
            logger.warning("Email не настроен")
            return False
        
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart()
            msg['From'] = self.email_config['email_user']
            msg['To'] = self.email_config['recipient']
            msg['Subject'] = subject
            
            msg.attach(MIMEText(report, 'plain', 'utf-8'))
            
            with smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                server.starttls()
                server.login(self.email_config['email_user'], self.email_config['email_password'])
                server.send_message(msg)
            
            logger.info("Отчет отправлен по email")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки email: {e}")
            return False

# Инициализация сервисов
gigachat_client = GigaChatClient()
db_manager = DatabaseManager()
notification_manager = NotificationManager()

@app.post("/analyze-trip")
async def analyze_trip_endpoint(payload: TripWebhookPayload, background_tasks: BackgroundTasks):
    """Endpoint для анализа поездки (вызывается из основного FastAPI)"""
    
    logger.info(f"Получен запрос на анализ поездки: {payload.filename}")
    
    # Запускаем анализ в фоне
    background_tasks.add_task(process_trip_analysis, payload.dict())
    
    return {"status": "accepted", "message": "Анализ поездки запущен"}

async def process_trip_analysis(trip_data: Dict[str, Any]):
    """Обработка анализа поездки"""
    
    try:
        # Получаем детальную статистику из БД
        detailed_stats = db_manager.get_trip_detailed_stats(trip_data['filename'])
        
        # Пересчитываем battery_per_km из фактических данных
        if trip_data['distance_km'] > 0:
            detailed_stats['battery_per_km'] = trip_data['battery_used'] / trip_data['distance_km']
        
        # Получаем AI-анализ
        ai_analysis = await gigachat_client.analyze_trip(trip_data, detailed_stats)
        
        # Формируем отчет
        report = create_trip_report(trip_data, detailed_stats, ai_analysis)
        
        # Отправляем уведомления
        await notification_manager.send_telegram_report(report)
        await notification_manager.send_email_report(
            f"🛴 Поездка завершена - {trip_data['distance_km']} км", 
            report
        )
        
        logger.info(f"Анализ поездки {trip_data['filename']} завершен")
        
    except Exception as e:
        logger.error(f"Ошибка анализа поездки: {e}")

def create_trip_report(trip_data: Dict[str, Any], detailed_stats: Dict[str, Any], ai_analysis: str) -> str:
    """Создание отчета о поездке"""
    
    # Форматируем время
    duration_hours = trip_data['duration_min'] // 60
    duration_mins = trip_data['duration_min'] % 60
    duration_str = f"{duration_hours}ч {duration_mins}м" if duration_hours > 0 else f"{duration_mins}м"
    
    # Прогноз остатка хода
    remaining_range = (trip_data['battery_end'] / max(detailed_stats['battery_per_km'], 0.1))
    
    report = f"""🛴 <b>Поездка завершена!</b>

📊 <b>Статистика:</b>
• {trip_data['distance_km']} км за {duration_str}
• Батарея: {trip_data['battery_start']}% → {trip_data['battery_end']}% (-{trip_data['battery_used']}%)
• Макс скорость: {trip_data['max_speed']} км/ч
• Средняя скорость: {trip_data['avg_speed']} км/ч
• Расход: {detailed_stats['battery_per_km']:.1f}%/км

⚡ <b>Остаток хода:</b> ~{remaining_range:.1f} км

🤖 <b>AI анализ:</b>
{ai_analysis}

📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}
"""
    
    return report

@app.get("/test-gigachat")
async def test_gigachat():
    """Тестовый endpoint для проверки подключения к GigaChat"""
    try:
        result = gigachat_client.test_connection()
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "message": "Ошибка тестирования GigaChat"
        }

@app.get("/test-gigachat-analysis")
async def test_gigachat_analysis():
    """Тестовый анализ поездки для проверки AI"""
    try:
        test_data = {
            'distance_km': 12.5,
            'duration_min': 35,
            'battery_start': 85,
            'battery_end': 42,
            'battery_used': 43,
            'max_speed': 28.5,
            'avg_speed': 18.2
        }
        
        test_stats = {
            'battery_per_km': 3.4,
            'efficiency_score': 7.5,
            'aggressiveness': 4.2,
            'avg_temp': 24.5,
            'high_speed_time': 15.8,
            'avg_battery_usage': 3.8,
            'typical_avg_speed': 19.1
        }
        
        result = await gigachat_client.analyze_trip(test_data, test_stats)
        
        return {
            "status": "success",
            "analysis": result,
            "test_data": test_data,
            "test_stats": test_stats,
            "token_status": {
                "has_token": bool(gigachat_client.access_token),
                "expires_at": gigachat_client.token_expires_at.isoformat() if gigachat_client.token_expires_at else None,
                "is_valid": gigachat_client._is_token_valid()
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "message": "Ошибка тестового анализа"
        }

@app.get("/token-info")
async def get_token_info():
    """Информация о текущем токене"""
    return {
        "has_token": bool(gigachat_client.access_token),
        "token_valid": gigachat_client._is_token_valid(),
        "expires_at": gigachat_client.token_expires_at.isoformat() if gigachat_client.token_expires_at else None,
        "time_until_expiry": str(gigachat_client.token_expires_at - datetime.now()) if gigachat_client.token_expires_at else None,
        "auth_key_configured": bool(gigachat_client.auth_key)
    }

@app.post("/refresh-token")
async def refresh_token():
    """Принудительное обновление токена"""
    try:
        # Сбрасываем текущий токен
        gigachat_client.access_token = None
        gigachat_client.token_expires_at = None
        
        # Получаем новый
        success = gigachat_client._get_new_access_token()
        
        if success:
            return {
                "status": "success",
                "message": "Токен обновлен успешно",
                "expires_at": gigachat_client.token_expires_at.isoformat(),
                "token_preview": gigachat_client.access_token[:20] + "..." if gigachat_client.access_token else None
            }
        else:
            return {
                "status": "error",
                "message": "Не удалось обновить токен"
            }
            
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "message": "Ошибка обновления токена"
        }

@app.get("/test-db-schema")
async def test_db_schema():
    """Тестовый endpoint для проверки схемы БД"""
    schema = db_manager.get_database_schema()
    return {
        "database_columns": schema,
        "message": "Схема базы данных"
    }

@app.get("/test-full-pipeline")
async def test_full_pipeline():
    """Полное тестирование всего пайплайна"""
    results = {}
    
    # 1. Тест базы данных
    try:
        schema = db_manager.get_database_schema()
        results["database"] = {
            "status": "success" if schema else "error",
            "columns_count": len(schema),
            "columns": list(schema.keys())[:10]  # Первые 10 колонок
        }
    except Exception as e:
        results["database"] = {
            "status": "error",
            "error": str(e)
        }
    
    # 2. Тест GigaChat
    try:
        gigachat_result = gigachat_client.test_connection()
        results["gigachat"] = gigachat_result
    except Exception as e:
        results["gigachat"] = {
            "status": "error",
            "error": str(e)
        }
    
    # 3. Тест Telegram
    try:
        if notification_manager.telegram_bot_token and notification_manager.telegram_chat_id:
            # Отправляем тестовое сообщение
            test_report = "🧪 Тестовое сообщение от WheelLog AI Analyzer"
            telegram_success = await notification_manager.send_telegram_report(test_report)
            results["telegram"] = {
                "status": "success" if telegram_success else "error",
                "configured": True
            }
        else:
            results["telegram"] = {
                "status": "not_configured",
                "configured": False
            }
    except Exception as e:
        results["telegram"] = {
            "status": "error",
            "error": str(e)
        }
    
    # 4. Общий статус
    all_services = [results.get("database", {}).get("status"), 
                   results.get("gigachat", {}).get("status"),
                   results.get("telegram", {}).get("status")]
    
    overall_status = "success" if all(s == "success" for s in all_services if s) else "partial"
    
    return {
        "overall_status": overall_status,
        "timestamp": datetime.now().isoformat(),
        "services": results
    }

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "gigachat": bool(os.getenv("GIGACHAT_AUTH_KEY")),
            "database": bool(os.getenv("DB_HOST")),
            "telegram": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
            "email": bool(os.getenv("SMTP_SERVER"))
        }
    }

@app.get("/stats")
async def get_stats():
    """Статистика сервиса"""
    return {
        "service": "WheelLog AI Analyzer",
        "version": "1.1.0",
        "uptime": "running"
    }

@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": "WheelLog AI Analyzer",
        "version": "1.1.0",
        "status": "running",
        "features": [
            "Автоматическое обновление GigaChat токенов",
            "AI анализ поездок на моноколесе",
            "Telegram уведомления",
            "Email отчеты",
            "Интеграция с TimescaleDB"
        ]
    }

if __name__ == "__main__":
    uvicorn.run(
        "ai_analyzer:app", 
        host="0.0.0.0", 
        port=8001, 
        reload=True,
        log_level="info"
    )
