#!/usr/bin/env python3
import os
import uuid
import requests
from dotenv import load_dotenv

load_dotenv()

def test_gigachat_auth():
    auth_key = os.getenv("GIGACHAT_AUTH_KEY")
    if not auth_key:
        print("❌ GIGACHAT_AUTH_KEY не установлен")
        return False
    
    oauth_url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    rq_uid = str(uuid.uuid4())
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'RqUID': rq_uid,
        'Authorization': f'Basic {auth_key}'
    }
    
    payload = {
        'scope': 'GIGACHAT_API_PERS'
    }
    
    try:
        print("🔄 Отправляем запрос на получение токена...")
        response = requests.post(oauth_url, headers=headers, data=payload, timeout=30, verify=False)
        
        if response.status_code == 200:
            token_data = response.json()
            print("✅ Токен получен успешно!")
            print(f"Access Token: {token_data['access_token'][:50]}...")
            print(f"Expires in: {token_data.get('expires_in', 'unknown')} seconds")
            
            # Тестируем API с полученным токеном
            api_url = "https://gigachat.devices.sberbank.ru/api/v1/models"
            api_headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {token_data["access_token"]}'
            }
            
            print("🔄 Тестируем API с полученным токеном...")
            api_response = requests.get(api_url, headers=api_headers, timeout=10, verify=False)
            
            if api_response.status_code == 200:
                models = api_response.json()
                print("✅ API работает! Доступные модели:")
                for model in models.get('data', []):
                    print(f"  - {model.get('id', 'Unknown')}")
                return True
            else:
                print(f"❌ Ошибка API: {api_response.status_code}")
                print(f"Response: {api_response.text}")
                return False
        else:
            print(f"❌ Ошибка получения токена: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return False

if __name__ == "__main__":
    test_gigachat_auth()
