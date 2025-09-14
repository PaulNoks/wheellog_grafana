#!/usr/bin/env python3
"""
WheelLog AI Analyzer Service
Сервис для AI-анализа поездок с интеграцией GigaChat
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import psycopg2
import requests
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="WheelLog AI Analyzer", version="1.0.0")

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
    """Клиент для работы с GigaChat API"""
    
    def __init__(self):
        self.api_url = "https://gigachat.devices.sberbank.ru/api/v1"
        self.token = os.getenv("GIGACHAT_TOKEN")
        if not self.token:
            raise ValueError("GIGACHAT_TOKEN не установлен в переменных окружения")
        
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
    
    async def analyze_trip(self, trip_data: Dict[str, Any], detailed_stats: Dict[str, Any]) -> str:
        """Анализ поездки через GigaChat"""
        
        prompt = self._create_analysis_prompt(trip_data, detailed_stats)
        
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
            response = requests.post(
                f"{self.api_url}/chat/completions",
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            return result['choices'][0]['message']['content']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса к GigaChat: {e}")
            return "❌ Не удалось получить AI-анализ"
    
    def _create_analysis_prompt(self, trip_data: Dict[str, Any], detailed_stats: Dict[str, Any]) -> str:
        """Создание промпта для анализа поездки"""
        
        return f"""
Проанализируй поездку на моноколесе:

📊 Основные данные:
- Расстояние: {trip_data['distance_km']} км
- Время: {trip_data['duration_min']} мин
- Батарея: {trip_data['battery_start']}% → {trip_data['battery_end']}% (-{trip_data['battery_used']}%)
- Макс скорость: {trip_data['max_speed']} км/ч
- Средняя скорость: {trip_data['avg_speed']} км/ч

📈 Детальная статистика:
- Расход батареи на км: {detailed_stats.get('battery_per_km', 0):.1f}%/км
- Эффективность: {detailed_stats.get('efficiency_score', 0):.1f}/10
- Агрессивность езды: {detailed_stats.get('aggressiveness', 0):.1f}/10
- Средняя температура: {detailed_stats.get('avg_temp', 0):.1f}°C
- Время на высокой скорости (>25 км/ч): {detailed_stats.get('high_speed_time', 0):.1f}%

📚 Сравнение с предыдущими поездками:
- Средний расход батареи: {detailed_stats.get('avg_battery_usage', 0):.1f}%/км
- Средняя скорость обычно: {detailed_stats.get('typical_avg_speed', 0):.1f} км/ч

Дай краткий анализ (до 300 символов):
1. Оценка эффективности поездки
2. Сравнение с обычным стилем езды
3. Практические рекомендации
4. Прогноз остатка хода при текущем заряде

Формат: эмодзи + короткие предложения, дружелюбный тон.
"""

class DatabaseManager:
    """Менеджер для работы с базой данных"""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'wheellog'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
    
    def get_trip_detailed_stats(self, filename: str) -> Dict[str, Any]:
        """Получение детальной статистики поездки из БД"""
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Получаем детальные данные поездки
            query = """
            WITH trip_data AS (
                SELECT 
                    speed, battery, temp, distance,
                    timestamp,
                    LAG(battery) OVER (ORDER BY timestamp) as prev_battery
                FROM wheellog_data 
                WHERE filename = %s 
                ORDER BY timestamp
            ),
            trip_stats AS (
                SELECT 
                    COUNT(*) as total_points,
                    AVG(temp) as avg_temp,
                    AVG(CASE WHEN speed > 25 THEN 1 ELSE 0 END) * 100 as high_speed_percentage,
                    STDDEV(speed) as speed_deviation,
                    MAX(distance) as total_distance,
                    (MAX(battery) - MIN(battery)) as battery_used,
                    EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp)))/60 as duration_min
                FROM trip_data
            ),
            historical_avg AS (
                SELECT 
                    AVG(battery_usage_per_km) as avg_battery_per_km,
                    AVG(avg_speed) as typical_avg_speed
                FROM (
                    SELECT 
                        filename,
                        (MAX(battery) - MIN(battery))::float / NULLIF(MAX(distance), 0) as battery_usage_per_km,
                        AVG(speed) as avg_speed
                    FROM wheellog_data 
                    WHERE timestamp > NOW() - INTERVAL '30 days'
                      AND filename != %s
                    GROUP BY filename
                    HAVING MAX(distance) > 1
                ) recent_trips
            )
            SELECT 
                ts.avg_temp,
                ts.high_speed_percentage,
                ts.speed_deviation,
                ts.total_distance,
                ts.battery_used,
                ts.duration_min,
                COALESCE(ha.avg_battery_per_km, 0) as avg_battery_usage,
                COALESCE(ha.typical_avg_speed, 0) as typical_avg_speed
            FROM trip_stats ts
            CROSS JOIN historical_avg ha
            """
            
            cursor.execute(query, (filename, filename))
            result = cursor.fetchone()
            
            if result:
                avg_temp, high_speed_pct, speed_dev, total_dist, battery_used, duration, avg_battery_usage, typical_speed = result
                
                # Вычисляем производные метрики
                battery_per_km = battery_used / max(total_dist, 0.1)
                efficiency_score = max(0, min(10, 10 - (battery_per_km - 3) * 2))  # Базовая оценка эффективности
                aggressiveness = min(10, max(0, speed_dev / 3))  # Агрессивность на основе разброса скорости
                
                return {
                    'avg_temp': avg_temp or 25,
                    'high_speed_time': high_speed_pct or 0,
                    'battery_per_km': battery_per_km,
                    'efficiency_score': efficiency_score,
                    'aggressiveness': aggressiveness,
                    'avg_battery_usage': avg_battery_usage or battery_per_km,
                    'typical_avg_speed': typical_speed or 18
                }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
        
        # Возвращаем значения по умолчанию
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

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "gigachat": bool(os.getenv("GIGACHAT_TOKEN")),
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
        "version": "1.0.0",
        "uptime": "running"
    }

if __name__ == "__main__":
    uvicorn.run(
        "ai_analyzer:app", 
        host="0.0.0.0", 
        port=8001, 
        reload=True,
        log_level="info"
    )

