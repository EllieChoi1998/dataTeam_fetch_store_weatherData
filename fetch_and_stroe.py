import os
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from pymongo import MongoClient
import json
from datetime import datetime, timedelta

# API 자격증명 정의
username = os.environ['API_USERNAME']
password = os.environ['API_PASSWORD']

# 현재 날짜 계산
today = datetime.utcnow()
start_date = today.strftime('%Y-%m-%dT00:00:00Z')
end_date = (today + timedelta(days=1) - timedelta(minutes=10)).strftime('%Y-%m-%dT23:50:00Z')
interval = 'PT30M'  # 30분 간격

# URL 구성
base_url = 'https://api.meteomatics.com'

# MongoDB 연결 설정
mongo_connection_string = os.environ['MONGO_URI']
client = MongoClient(mongo_connection_string)

# 데이터베이스와 컬렉션 선택
db = client['project']
airport_collection = db['airport_data']  # 공항 데이터를 저장한 컬렉션
weather_collection = db['weather_data']  # 날씨 데이터를 저장할 컬렉션

# 공항 데이터 가져오기
airports = list(airport_collection.find({}, {'name': 1, 'latitude_deg': 1, 'longitude_deg': 1, '_id': 0}))

# 공항 데이터가 존재하지 않는 경우 종료
if not airports:
    print("공항 데이터가 없습니다.")
    exit()

all_parameters = [
    'wind_speed_10m:ms',
    'wind_dir_10m:d',
    'wind_gusts_10m_1h:ms',
    'wind_gusts_10m_24h:ms',
    't_2m:C',
    't_max_2m_24h:C',
    't_min_2m_24h:C',
    'msl_pressure:hPa',
    'precip_1h:mm',
    'precip_24h:mm',
    'uv:idx',
    'sunrise:sql',
    'sunset:sql'
]

weather_data = []

# 각 공항 위치에 대해 데이터 요청
for airport in airports:
    location_str = f"{airport['latitude_deg']},{airport['longitude_deg']}"
    for i in range(0, len(all_parameters), 10):
        parameters = all_parameters[i:i + 10]
        parameters_str = ','.join(parameters)
        url = f"{base_url}/{start_date}--{end_date}:{interval}/{parameters_str}/{location_str}/json"

        # GET 요청
        response = requests.get(url, auth=HTTPBasicAuth(username, password))

        # 응답 상태 확인
        if response.status_code == 200:
            data = response.json()
            print(f"데이터 성공적으로 가져옴 (위치 {location_str}, 파라미터 그룹 {i // 10 + 1}):", json.dumps(data, indent=2))
            
            # 데이터 처리
            for entry in data['data']:
                for timestamp in entry['coordinates'][0]['dates']:
                    weather_record = {
                        'timestamp': timestamp['date'],
                        'parameter': entry['parameter'],
                        'value': timestamp['value'],
                        'location_name': airport['name'],
                        'latitude': airport['latitude_deg'],
                        'longitude': airport['longitude_deg']
                    }
                    weather_data.append(weather_record)
        else:
            print(f"데이터 가져오기 실패 (위치 {location_str}, 파라미터 그룹 {i // 10 + 1}): {response.status_code}, {response.text}")

# 데이터프레임으로 변환 후 MongoDB에 삽입
if weather_data:
    weather_df = pd.DataFrame(weather_data)
    data_dict = weather_df.to_dict("records")
    weather_collection.insert_many(data_dict)
    print("날씨 데이터를 MongoDB에 성공적으로 삽입했습니다.")
else:
    print("삽입할 데이터가 없습니다.")
