import os
import pandas as pd
import logging
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "wheellog")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./uploads")

app = FastAPI()

os.makedirs(UPLOAD_DIR, exist_ok=True)

def get_db_connection():
    """Получение подключения к базе данных"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        logger.info("✅ Подключение к БД успешно")
        return conn
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        raise

def create_hypertable():
    """Создание hypertable для TimescaleDB"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Создаем обычную таблицу
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wheellog_data (
                timestamp TIMESTAMPTZ NOT NULL,
                file_name TEXT,
                latitude DOUBLE PRECISION DEFAULT 0,
                longitude DOUBLE PRECISION DEFAULT 0,
                gps_speed DOUBLE PRECISION DEFAULT 0,
                gps_alt DOUBLE PRECISION DEFAULT 0,
                gps_heading DOUBLE PRECISION DEFAULT 0,
                gps_distance DOUBLE PRECISION DEFAULT 0,
                speed DOUBLE PRECISION DEFAULT 0,
                voltage DOUBLE PRECISION DEFAULT 0,
                phase_current DOUBLE PRECISION DEFAULT 0,
                current DOUBLE PRECISION DEFAULT 0,
                power DOUBLE PRECISION DEFAULT 0,
                torque DOUBLE PRECISION DEFAULT 0,
                pwm DOUBLE PRECISION DEFAULT 0,
                battery_level DOUBLE PRECISION DEFAULT 0,
                distance DOUBLE PRECISION DEFAULT 0,
                totaldistance DOUBLE PRECISION DEFAULT 0,
                system_temp DOUBLE PRECISION DEFAULT 0,
                temp2 DOUBLE PRECISION DEFAULT 0,
                tilt DOUBLE PRECISION DEFAULT 0,
                roll DOUBLE PRECISION DEFAULT 0,
                mode TEXT,
                alert TEXT
            );
        """)

        # Проверяем, является ли таблица уже hypertable
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM _timescaledb_catalog.hypertable
                WHERE table_name = 'wheellog_data'
            );
        """)

        is_hypertable = cursor.fetchone()[0]

        if not is_hypertable:
            logger.info("Создаем hypertable...")
            cursor.execute("SELECT create_hypertable('wheellog_data', 'timestamp');")

            cursor.execute("""
                ALTER TABLE wheellog_data SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'file_name'
                );
            """)

            cursor.execute("""
                SELECT add_compression_policy('wheellog_data', INTERVAL '7 days');
            """)

            logger.info("✅ Hypertable создана и настроена!")
        else:
            logger.info("✅ Hypertable уже существует")

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка при создании hypertable: {e}")
        if 'conn' in locals():
            conn.rollback()
            cursor.close()
            conn.close()
        raise

def _parse_timestamp(date_str: str, time_str: str) -> datetime:
    """Парсинг timestamp из wheellog формата"""
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S.%f")
        return dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
    except ValueError:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=datetime.now().astimezone().tzinfo)

def _ingest_to_timescale(file_path: str, filename: str):
    """Загрузка данных в TimescaleDB с подробным логированием"""
    logger.info(f"📥 Начинаем загрузку файла: {filename}")
    
    try:
        # Проверяем существование файла
        if not os.path.exists(file_path):
            logger.error(f"❌ Файл не найден: {file_path}")
            return
            
        # Читаем CSV
        logger.info(f"📖 Читаем CSV файл: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"📊 Прочитано {len(df)} строк из {filename}")
        
        # Выводим первые несколько строк для отладки
        logger.info(f"🔍 Колонки в файле: {list(df.columns)}")
        logger.info(f"🔍 Первые 3 строки:\n{df.head(3)}")

        # Проверяем обязательные колонки
        required_columns = ['date', 'time']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"❌ Отсутствуют обязательные колонки: {missing_columns}")
            return

        # Нормализация данных
        numeric_columns = ["latitude","longitude","gps_speed","gps_alt","gps_heading","gps_distance",
                          "speed","voltage","phase_current","current","power","torque","pwm",
                          "battery_level","distance","totaldistance","system_temp","temp2","tilt","roll"]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Подключаемся к БД
        logger.info("🔌 Подключаемся к базе данных...")
        conn = get_db_connection()
        cursor = conn.cursor()

        # Подготовка данных для вставки
        insert_data = []
        errors_count = 0

        logger.info("🔄 Обрабатываем строки...")
        for i, row in df.iterrows():
            try:
                ts = _parse_timestamp(str(row["date"]), str(row["time"]))
                insert_data.append((
                    ts, filename,
                    float(row.get("latitude", 0)), float(row.get("longitude", 0)),
                    float(row.get("gps_speed", 0)), float(row.get("gps_alt", 0)),
                    float(row.get("gps_heading", 0)), float(row.get("gps_distance", 0)),
                    float(row.get("speed", 0)), float(row.get("voltage", 0)),
                    float(row.get("phase_current", 0)), float(row.get("current", 0)),
                    float(row.get("power", 0)), float(row.get("torque", 0)),
                    float(row.get("pwm", 0)), float(row.get("battery_level", 0)),
                    float(row.get("distance", 0)), float(row.get("totaldistance", 0)),
                    float(row.get("system_temp", 0)), float(row.get("temp2", 0)),
                    float(row.get("tilt", 0)), float(row.get("roll", 0)),
                    str(row.get("mode", "")), str(row.get("alert", ""))
                ))
            except Exception as e:
                errors_count += 1
                logger.warning(f"⚠️ Ошибка в строке {i}: {e}")
                if errors_count <= 3:  # Показываем первые 3 ошибки
                    logger.warning(f"⚠️ Проблемная строка: {row}")
                continue

        # Вставляем данные
        if insert_data:
            logger.info(f"💾 Вставляем {len(insert_data)} записей в БД...")
            insert_query = """

                    INSERT INTO wheellog_data (
                    timestamp, file_name, latitude, longitude, gps_speed, gps_alt,
                    gps_heading, gps_distance, speed, voltage, phase_current, current,
                    power, torque, pwm, battery_level, distance, totaldistance,
                    system_temp, temp2, tilt, roll, mode, alert
                ) VALUES %s
            """

            execute_values(cursor, insert_query, insert_data,
                          template=None, page_size=5000)

            conn.commit()
            logger.info(f"✅ Успешно загружено {len(insert_data)} записей из {filename}")
            
            # Проверяем, что данные действительно вставились
            cursor.execute("SELECT COUNT(*) FROM wheellog_data WHERE file_name = %s", (filename,))
            count_in_db = cursor.fetchone()[0]
            logger.info(f"🔍 Проверка: в БД найдено {count_in_db} записей для файла {filename}")
            
        else:
            logger.error("❌ Нет данных для вставки!")

        if errors_count > 0:
            logger.warning(f"⚠️ Пропущено {errors_count} строк с ошибками")

        cursor.close()
        conn.close()
        logger.info(f"✅ Обработка файла {filename} завершена")

    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА при обработке файла {filename}: {e}")
        logger.exception("Полная трассировка ошибки:")

@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    logger.info("🚀 Запуск TimescaleDB API...")
    try:
        create_hypertable()
    except Exception as e:
        logger.error(f"❌ Ошибка при инициализации: {e}")

@app.post("/upload")
async def upload_csv(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """Загрузка CSV файла"""
    logger.info(f"📤 Получен файл для загрузки: {file.filename}")
    
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file.")

    dst_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(dst_path, "wb") as f:
        f.write(content)
    
    logger.info(f"💾 Файл сохранен в: {dst_path}")
    logger.info(f"📊 Размер файла: {len(content)} bytes")

    # Добавляем фоновую задачу
    background_tasks.add_task(_ingest_to_timescale, dst_path, file.filename)
    logger.info(f"🔄 Фоновая задача добавлена для файла: {file.filename}")
    
    return JSONResponse({
        "status": "ok", 
        "file": file.filename, 
        "message": "File uploaded and processing started",
        "file_size": len(content),
        "file_path": dst_path
    })

# Добавляем новый эндпоинт для синхронной загрузки (для отладки)
@app.post("/upload-sync")
async def upload_csv_sync(file: UploadFile = File(...)):
    """Синхронная загрузка CSV файла (для отладки)"""
    logger.info(f"📤 СИНХРОННАЯ загрузка файла: {file.filename}")
    
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file.")

    dst_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(dst_path, "wb") as f:
        f.write(content)
    
    logger.info(f"💾 Файл сохранен в: {dst_path}")
    
    try:
        # Обрабатываем файл синхронно
        _ingest_to_timescale(dst_path, file.filename)
        return JSONResponse({
            "status": "ok", 
            "file": file.filename, 
            "message": "File uploaded and processed successfully"
        })
    except Exception as e:
        logger.error(f"❌ Ошибка при синхронной обработке: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@app.get("/stats")
async def get_stats():
    """Статистика с использованием TimescaleDB функций"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Общее количество записей
        cursor.execute("SELECT COUNT(*) FROM wheellog_data")
        total_records = cursor.fetchone()[0]

        # Статистика по файлам
        cursor.execute("SELECT file_name, COUNT(*) FROM wheellog_data GROUP BY file_name ORDER BY COUNT(*) DESC")
        files_stats = cursor.fetchall()

        # Временной диапазон
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM wheellog_data")
        time_range = cursor.fetchone()

        # TimescaleDB chunks информация
        cursor.execute("""
            SELECT
                chunk_name,
                range_start,
                range_end,
                is_compressed
            FROM timescaledb_information.chunks
            WHERE hypertable_name = 'wheellog_data'
            ORDER BY range_start DESC
            LIMIT 10
        """)
        chunks_info = cursor.fetchall()

        result = {
            "total_records": total_records,
            "files_count": len(files_stats),
            "files": dict(files_stats[:10]),
            "time_range": {
                "from": time_range[0],
                "to": time_range[1]
            },
            "chunks_count": len(chunks_info),
            "recent_chunks": [
                {
                    "name": chunk[0],
                    "start": chunk[1],
                    "end": chunk[2],
                    "compressed": chunk[3]
                } for chunk in chunks_info
            ]
        }
        
        cursor.close()
        conn.close()
        return result

    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/health")
async def health_check():
    """Проверка работоспособности API и БД"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"❌ Ошибка health check: {e}")
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")

# Запуск: uvicorn main:app --host 0.0.0.0 --port 8000 --reload
