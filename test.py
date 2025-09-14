#!/usr/bin/env python3
"""
Тестирование локальной интеграции WheelLog Analytics
"""

import requests
import json
import time
from datetime import datetime

# Конфигурация
FASTAPI_URL = "http://localhost:8000"
N8N_URL = "http://192.168.0.193:5678"  # IP вашего LXC контейнера
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"  # Замените на ваш Chat ID

def test_fastapi_health():
    """Тест здоровья FastAPI"""
    print("🔍 Тестируем FastAPI...")
    try:
        response = requests.get(f"{FASTAPI_URL}/health", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ FastAPI работает: {data['status']}")
            return True
        else:
            print(f"❌ FastAPI ошибка: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ FastAPI недоступен: {e}")
        return False

def test_n8n_webhook():
    """Тест n8n webhook с локальным анализом"""
    print("\n🔍 Тестируем n8n webhook с локальным анализом...")
    
    test_data = {
        "filename": "test_ride_local_2025_01_27.csv",
        "timestamp": datetime.now().isoformat(),
        "records_count": 1800,
        "distance_km": 12.5,
        "distance_m": 12500,
        "duration_min": 45.0,
        "duration_sec": 2700,
        "battery_start": 87.0,
        "battery_end": 34.0,
        "battery_used": 53.0,
        "battery_efficiency": 4.24,
        "max_speed": 28.0,
        "avg_speed": 18.2,
        "avg_voltage": 84.2,
        "max_current": 25.5,
        "avg_current": 12.3,
        "avg_temp": 42.1,
        "energy_efficiency": 15.2,
        "has_gps": True,
        "data_quality": "good",
        "data_completeness": {
            "speed_records": 0.95,
            "battery_records": 0.98,
            "gps_records": 0.92
        },
        "chat_id": TELEGRAM_CHAT_ID,
        "source": "wheellog_analytics_local",
        "version": "2.1.0"
    }
    
    try:
        response = requests.post(
            f"{N8N_URL}/webhook/wheellog-webhook",
            json=test_data,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ n8n webhook работает: {result}")
            return True
        else:
            print(f"❌ n8n webhook ошибка: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ n8n webhook недоступен: {e}")
        return False

def test_fastapi_n8n_integration():
    """Тест интеграции FastAPI → n8n"""
    print("\n🔍 Тестируем интеграцию FastAPI → n8n...")
    
    try:
        response = requests.post(f"{FASTAPI_URL}/test-n8n", timeout=30)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Интеграция работает: {result['status']}")
            if result.get('telegram_configured'):
                print("✅ Telegram настроен")
            else:
                print("⚠️ Telegram не настроен")
            return True
        else:
            print(f"❌ Интеграция ошибка: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Интеграция недоступна: {e}")
        return False

def test_short_trip():
    """Тест короткой поездки (должна быть пропущена)"""
    print("\n🔍 Тестируем короткую поездку...")
    
    short_trip_data = {
        "filename": "short_ride_local.csv",
        "timestamp": datetime.now().isoformat(),
        "records_count": 30,
        "distance_km": 0.2,  # Меньше 0.5 км
        "duration_min": 0.5,
        "battery_start": 100.0,
        "battery_end": 99.0,
        "battery_used": 1.0,
        "max_speed": 5.0,
        "avg_speed": 3.0,
        "chat_id": TELEGRAM_CHAT_ID
    }
    
    try:
        response = requests.post(
            f"{N8N_URL}/webhook/wheellog-webhook",
            json=short_trip_data,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('status') == 'skipped':
                print("✅ Короткая поездка корректно пропущена")
                return True
            else:
                print(f"⚠️ Неожиданный результат: {result}")
                return False
        else:
            print(f"❌ Ошибка теста короткой поездки: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Ошибка теста короткой поездки: {e}")
        return False

def test_different_riding_styles():
    """Тест разных стилей катания"""
    print("\n🔍 Тестируем разные стили катания...")
    
    styles = [
        {"name": "спокойный", "max_speed": 15, "avg_speed": 10},
        {"name": "умеренный", "max_speed": 22, "avg_speed": 18},
        {"name": "активный", "max_speed": 28, "avg_speed": 22},
        {"name": "агрессивный", "max_speed": 35, "avg_speed": 28}
    ]
    
    results = []
    
    for style in styles:
        test_data = {
            "filename": f"test_{style['name']}_style.csv",
            "timestamp": datetime.now().isoformat(),
            "records_count": 1200,
            "distance_km": 8.5,
            "duration_min": 30,
            "battery_start": 90.0,
            "battery_end": 60.0,
            "battery_used": 30.0,
            "battery_efficiency": 3.5,
            "max_speed": style["max_speed"],
            "avg_speed": style["avg_speed"],
            "avg_temp": 40.0,
            "data_quality": "good",
            "chat_id": TELEGRAM_CHAT_ID
        }
        
        try:
            response = requests.post(
                f"{N8N_URL}/webhook/wheellog-webhook",
                json=test_data,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Стиль '{style['name']}': {result.get('status', 'unknown')}")
                results.append(True)
            else:
                print(f"❌ Стиль '{style['name']}': ошибка {response.status_code}")
                results.append(False)
                
        except Exception as e:
            print(f"❌ Стиль '{style['name']}': {e}")
            results.append(False)
        
        time.sleep(1)  # Пауза между тестами
    
    return all(results)

def main():
    """Основная функция тестирования"""
    print("🚀 Запуск тестирования WheelLog Analytics (локальная версия)")
    print("=" * 70)
    
    # Обновляем конфигурацию из переменных окружения
    import os
    global TELEGRAM_CHAT_ID, N8N_URL, FASTAPI_URL
    
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', TELEGRAM_CHAT_ID)
    N8N_URL = os.getenv('N8N_URL', N8N_URL)
    FASTAPI_URL = os.getenv('FASTAPI_URL', FASTAPI_URL)
    
    print(f"📡 FastAPI URL: {FASTAPI_URL}")
    print(f"🔗 n8n URL: {N8N_URL}")
    print(f"💬 Telegram Chat ID: {'настроен' if TELEGRAM_CHAT_ID != 'YOUR_CHAT_ID' else 'НЕ НАСТРОЕН'}")
    print()
    
    tests = [
        ("FastAPI Health", test_fastapi_health),
        ("n8n Webhook (Local Analysis)", test_n8n_webhook),
        ("FastAPI → n8n Integration", test_fastapi_n8n_integration),
        ("Short Trip Filter", test_short_trip),
        ("Different Riding Styles", test_different_riding_styles)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Критическая ошибка в {test_name}: {e}")
            results.append((test_name, False))
        
        time.sleep(1)  # Небольшая пауза между тестами
    
    # Итоговый отчет
    print("\n" + "="*70)
    print("📊 ИТОГОВЫЙ ОТЧЕТ")
    print("="*70)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n📈 Результат: {passed}/{total} тестов пройдено")
    
    if passed == total:
        print("🎉 Все тесты пройдены! Локальная интеграция работает корректно.")
        print("\n💡 Преимущества локального анализа:")
        print("   • Безопасность - данные не покидают ваш сервер")
        print("   • Скорость - мгновенный анализ без API вызовов")
        print("   • Надежность - работает без интернета")
        print("   • Экономия - нет затрат на AI API")
    else:
        print("⚠️ Некоторые тесты провалены. Проверьте настройки.")
        
        if not any(result for _, result in results[:2]):  # FastAPI и n8n
            print("\n💡 Рекомендации:")
            print("1. Убедитесь, что FastAPI запущен: sudo systemctl status wheellog-api.service")
            print("2. Проверьте, что n8n доступен в LXC контейнере")
            print("3. Убедитесь, что workflow активирован в n8n")
            print("4. Проверьте настройки credentials в n8n")

if __name__ == "__main__":
    main()

