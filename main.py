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
logger = logging.getLogger(__name__)  # Исправлено: было name

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

    def __init__(self):  # Исправлено: было init
        self.db_config = DB_CONFIG

    def parse_wheellog_csv(self, file_path: str, filename: str) -> dict:
        """Парсинг CSV файла WheelLog"""
        try:
            logger.info(f"Парсим CSV файл: {filename}")
            
            # Читаем CSV файл
            df = pd.read_csv(file_path)
            
            logger.info(f"Файл прочитан, строк: {len(df)}, колонок: {len(df.columns)}")
            logger.info(f"Колонки в файле: {list(df.columns)}")

            # Проверяем наличие основных колонок (гибкая проверка)
            available_columns = df.columns.tolist()
            
            # Ищем колонки с данными (возможны разные названия)
            time_col = None
            speed_col = None  
            battery_col = None
            distance_col = None
            
            for col in available_columns:
                col_lower = col.lower()
                if 'time' in col_lower or 'timestamp' in col_lower:
                    time_col = col
                elif 'speed' in col_lower or 'velocity' in col_lower:
                    speed_col = col
                elif 'battery' in col_lower or 'bat' in col_lower or 'charge' in col_lower:
                    battery_col = col
                elif 'distance' in col_lower or 'dist' in col_lower or 'km' in col_lower:
                    distance_col = col

            logger.info(f"Найденные колонки: time={time_col}, speed={speed_col}, battery={battery_col}, distance={distance_col}")

            # Если основные колонки не найдены, используем первые доступные
            if not time_col and len(available_columns) > 0:
                time_col = available_columns[0]
                logger.warning(f"Колонка времени не найдена, используем: {time_col}")
            
            if not speed_col and len(available_columns) > 1:
                speed_col = available_columns[1]
                logger.warning(f"Колонка скорости не найдена, используем: {speed_col}")

            # Преобразуем timestamp
            if time_col:
                try:
                    df['timestamp'] = pd.to_datetime(df[time_col])
                    logger.info("Timestamp успешно преобразован")
                except Exception as e:
                    logger.error(f"Ошибка преобразования timestamp: {e}")
                    # Создаем искусственный timestamp
                    df['timestamp'] = pd.date_range(start=datetime.now(), periods=len(df), freq='1S')
            
            df['filename'] = filename

            # Вычисляем статистику поездки
            trip_stats = self._calculate_trip_stats(df, speed_col, battery_col, distance_col)

            return {
                'dataframe': df,
                'stats': trip_stats,
                'records_count': len(df)
            }

        except Exception as e:
            logger.error(f"Ошибка парсинга {filename}: {e}")
            raise

    def _calculate_trip_stats(self, df: pd.DataFrame, speed_col: str, battery_col: str, distance_col: str) -> dict:
        """Вычисление статистики поездки"""
        try:
            logger.info("Вычисляем статистику поездки")
            
            # Основные метрики с проверкой наличия колонок
            distance_km = 0.0
            battery_start = 100
            battery_end = 100  
            battery_used = 0
            max_speed = 0.0
            avg_speed = 0.0

            # Расстояние
            if distance_col and distance_col in df.columns:
                try:
                    if 'total' in distance_col.lower():
            # Для totaldistance - вычисляем разность
                        distance_start = float(df[distance_col].min()) 
                        distance_end = float(df[distance_col].max())
                        distance_raw = distance_end - distance_start
                    else:
            # Для distance - берем максимальное значение
                        distance_raw = float(df[distance_col].max())
        
                    distance_km = distance_raw / 1000  # Всегда переводим в км
                    logger.info(f"Расстояние поездки: {distance_raw:.0f}м -> {distance_km:.2f} км")
                except:
                    distance_km = 0.0
            # Время поездки
            try:
                duration_min = int((df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 60)
                logger.info(f"Длительность: {duration_min} мин")
            except:
                duration_min = len(df)  # Примерно по количеству записей

            # Батарея
            if battery_col and battery_col in df.columns:
                try:
                    battery_start = int(df[battery_col].iloc[0])
                    battery_end = int(df[battery_col].iloc[-1])
                    battery_used = battery_start - battery_end
                    logger.info(f"Батарея: {battery_start}% -> {battery_end}% (-{battery_used}%)")
                except:
                    logger.warning("Ошибка обработки данных батареи")

            # Скорость
            if speed_col and speed_col in df.columns:
                try:
                    max_speed = float(df[speed_col].max())
                    avg_speed = float(df[speed_col].mean())
                    logger.info(f"Скорость: макс {max_speed}, средн {avg_speed}")
                except:
                    logger.warning("Ошибка обработки данных скорости")

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
            logger.info("Подключаемся к базе данных...")
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Проверяем структуру таблицы
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'wheellog_data'
                ORDER BY ordinal_position;
            """)
            
            db_columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"Колонки в БД: {db_columns}")

            if not db_columns:
                logger.error("Таблица wheellog_data не найдена или пуста")
                cursor.close()
                conn.close()
                return False

            # Подготавливаем данные для вставки только с существующими колонками
            df_columns = df.columns.tolist()
            valid_columns = [col for col in df_columns if col in db_columns]
            
            logger.info(f"Будем вставлять колонки: {valid_columns}")
            
            if not valid_columns:
                logger.error("Нет совпадающих колонок между CSV и БД")
                cursor.close()
                conn.close()
                return False

            # Формируем запрос
            placeholders = ', '.join(['%s'] * len(valid_columns))
            column_names = ', '.join(valid_columns)

            insert_query = f"""
                INSERT INTO wheellog_data ({column_names}) 
                VALUES ({placeholders})
            """

            # Подготавливаем данные
            df_subset = df[valid_columns]
            data_tuples = [tuple(row) for row in df_subset.values]
            
            logger.info(f"Подготовлено {len(data_tuples)} записей для вставки")

            # Выполняем batch insert
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            
            inserted_count = cursor.rowcount
            logger.info(f"Успешно вставлено записей в БД: {inserted_count}")

            cursor.close()
            conn.close()

            return inserted_count > 0

        except Exception as e:
            logger.error(f"Ошибка сохранения в БД: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
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

        logger.info(f"Файл сохранен: {file.filename}, размер: {len(content)} байт")

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

    try:  # Исправлен отступ
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
                COUNT(*) as active_records
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

        return {  # Исправлен отступ
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

if __name__ == "__main__":  # Исправлено: было name
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

