#!/usr/bin/env python3
"""
Обновленный main.py с интеграцией AI анализатора
Заменяет существующий ~/server/main.py
"""

import os
import logging
import pandas as pd
import psycopg2
import requests
import json
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import asyncio

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="WheelLog Data Processor", version="2.0.0")

# Настройки
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'wheellog'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

UPLOAD_DIR = os.getenv('UPLOAD_DIR', './uploads')
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL', '')
AI_ANALYZER_URL = "http://localhost:8001/analyze-trip"

# Создаем директорию для загрузок
os.makedirs(UPLOAD_DIR, exist_ok=True)

class WheelLogProcessor:
    """Класс для обработки данных WheelLog"""
    
    def __init__(self):
        self.db_config = DB_CONFIG
    
    def parse_wheellog_csv(self, file_path: str, filename: str) -> dict:
        """Парсинг CSV файла WheelLog"""
        try:
            # Читаем CSV файл
            df = pd.read_csv(file_path)
            
            # Базовая валидация структуры
            required_columns = ['timestamp', 'speed', 'battery', 'distance']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Отсутствуют обязательные колонки: {missing_columns}")
            
            # Преобразуем timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['filename'] = filename
            
            # Вычисляем статистику поездки
            trip_stats = self._calculate_trip_stats(df)
            
            return {
                'dataframe': df,
                'stats': trip_stats,
                'records_count': len(df)
            }
            
        except Exception as e:
            logger.error(f"Ошибка парсинга {filename}: {e}")
            raise
    
    def _calculate_trip_stats(self, df: pd.DataFrame) -> dict:
        """Вычисление статистики поездки"""
        try:
            # Основные метрики
            distance_km = float(df['distance'].max())
            duration_min = int((df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 60)
            
            battery_start = int(df['battery'].iloc[0])
            battery_end = int(df['battery'].iloc[-1])
            battery_used = battery_start - battery_end
            
            max_speed = float(df['speed'].max())
            avg_speed = float(df['speed'].mean())
            
            return {
                'distance_km': round(distance_km, 2),
                'duration_min': duration_min,
                'battery_start': battery_start,
                'battery_end': battery_end,
                'battery_used': battery_used,
                'max_speed': round(max_speed, 1),
                'avg_speed': round(avg_speed, 1)
            }
            
        except Exception as e:
            logger.error(f"Ошибка вычисления статистики: {e}")
            return {
                'distance_km': 0.0,
                'duration_min': 0,
                'battery_start': 100,
                'battery_end': 100,
                'battery_used': 0,
                'max_speed': 0.0,
                'avg_speed': 0.0
            }
    
    def save_to_database(self, df: pd.DataFrame) -> bool:
        """Сохранение данных в TimescaleDB"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Подготавливаем данные для вставки
            columns = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(columns)
            
            insert_query = f"""
                INSERT INTO wheellog_data ({column_names}) 
                VALUES ({placeholders})
                ON CONFLICT (timestamp, filename) DO NOTHING
            """
            
            # Конвертируем DataFrame в список кортежей
            data_tuples = [tuple(row) for row in df.values]
            
            # Выполняем batch insert
            cursor.executemany(insert_query, data_tuples)
            
            conn.commit()
            inserted_count = cursor.rowcount
            
            logger.info(f"Вставлено записей в БД: {inserted_count}")
            
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в БД: {e}")
            return False

async def send_webhook_notification(webhook_url: str, payload: dict):
    """Отправка webhook уведомления в n8n"""
    if not webhook_url:
        logger.warning("N8N Webhook URL не настроен")
        return
    
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info("Webhook успешно отправлен в n8n")
        else:
            logger.warning(f"Ошибка webhook n8n: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Не удалось отправить webhook в n8n: {e}")

async def send_to_ai_analyzer(trip_data: dict):
    """Отправка данных поездки в AI анализатор"""
    try:
        response = requests.post(
            AI_ANALYZER_URL,
            json=trip_data,
            timeout=5
        )
        
        if response.status_code == 200:
            logger.info("Данные отправлены в AI анализатор")
        else:
            logger.warning(f"Ошибка отправки в AI анализатор: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Не удалось отправить данные в AI анализатор: {e}")

# Инициализация процессора
processor = WheelLogProcessor()

@app.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """Загрузка и обработка CSV файла WheelLog"""
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Поддерживаются только CSV файлы")
    
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    
    try:
        # Сохраняем загруженный файл
        with open(file_path, 'wb') as f:
            content = await file.read()
            f.write(content)
        
        logger.info(f"Файл сохранен: {file.filename}")
        
        # Запускаем обработку в фоне
        background_tasks.add_task(process_wheellog_file, file_path, file.filename)
        
        return {
            "status": "accepted",
            "filename": file.filename,
            "message": "Файл принят в обработку"
        }
        
    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")

@app.post("/upload-sync")
async def upload_file_sync(file: UploadFile = File(...)):
    """Синхронная загрузка и обработка CSV файла"""
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Поддерживаются только CSV файлы")
    
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    
    try:
        # Сохраняем файл
        with open(file_path, 'wb') as f:
            content = await file.read()
            f.write(content)
        
        # Обрабатываем сразу
        result = await process_wheellog_file(file_path, file.filename)
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка синхронной обработки: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка обработки: {str(e)}")

async def process_wheellog_file(file_path: str, filename: str) -> dict:
    """Полная обработка файла WheelLog"""
    
    try:
        logger.info(f"Начинаем обработку: {filename}")
        
        # Парсим CSV
        parsed_data = processor.parse_wheellog_csv(file_path, filename)
        df = parsed_data['dataframe']
        stats = parsed_data['stats']
        
        # Сохраняем в БД
        save_success = processor.save_to_database(df)
        
        if not save_success:
            logger.error(f"Не удалось сохранить {filename} в БД")
            return {"status": "error", "message": "Ошибка сохранения в БД"}
        
        # Формируем payload для уведомлений
        webhook_payload = {
            "filename": filename,
            "timestamp": datetime.now().isoformat(),
            "records_count": parsed_data['records_count'],
            **stats
        }
        
        # Отправляем уведомления
        await send_webhook_notification(N8N_WEBHOOK_URL, webhook_payload)
        await send_to_ai_analyzer(webhook_payload)
        
        logger.info(f"Обработка {filename} завершена успешно")
        
        return {
            "status": "success",
            "filename": filename,
            "records_processed": parsed_data['records_count'],
            "trip_stats": stats
        }
        
    except Exception as e:
        logger.error(f"Ошибка обработки {filename}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/stats")
async def get_stats():
    """Получение статистики системы"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Общая статистика
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT filename) as total_trips,
                COUNT(*) as total_records,
                MIN(timestamp) as first_record,
                MAX(timestamp) as last_record
            FROM wheellog_data
        """)
        
        result = cursor.fetchone()
        total_trips, total_records, first_record, last_record = result
        
        # Статистика за последние 30 дней
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT filename) as recent_trips,
                SUM(CASE WHEN speed > 0 THEN 1 ELSE 0 END) as active_records
            FROM wheellog_data 
            WHERE timestamp > NOW() - INTERVAL '30 days'
        """)
        
        recent_result = cursor.fetchone()
        recent_trips, active_records = recent_result
        
        cursor.close()
        conn.close()
        
        return {
            "status": "healthy",
            "database": {
                "total_trips": total_trips or 0,
                "total_records": total_records or 0,
                "recent_trips_30d": recent_trips or 0,
                "active_records_30d": active_records or 0,
                "first_record": first_record.isoformat() if first_record else None,
                "last_record": last_record.isoformat() if last_record else None
            },
            "services": {
                "n8n_webhook": bool(N8N_WEBHOOK_URL),
                "ai_analyzer": True
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    try:
        # Проверяем соединение с БД
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        
        # Проверяем AI анализатор
        ai_status = "unknown"
        try:
            response = requests.get(f"{AI_ANALYZER_URL.replace('/analyze-trip', '/health')}", timeout=3)
            ai_status = "healthy" if response.status_code == 200 else "error"
        except:
            ai_status = "offline"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {
                "database": "healthy",
                "ai_analyzer": ai_status,
                "file_upload": "healthy"
            }
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": "WheelLog Data Processor",
        "version": "2.0.0",
        "status": "running",
        "features": [
            "CSV файл обработка",
            "TimescaleDB интеграция", 
            "N8N webhook уведомления",
            "AI анализ поездок",
            "Автоматическая отчетность"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
