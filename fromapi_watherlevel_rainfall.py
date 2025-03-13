import time
import oracledb
import requests
from apscheduler.schedulers.background import BackgroundScheduler

# Oracle DB 연결 정보
DB_CONFIG = {
    "user": "SAMA",
    "password": "SAMA1234",
    "dsn": "192.168.100.30:1522/XE"
}

# OpenAPI 키
APP_ID = "C03BC3D1-443D-46EE-A91F-168CEFDD65A6"

# DB 연결 함수
def get_db_connection():
    try:
        connection = oracledb.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        print(f"DB 연결 오류: {e}")
        return None

# 강수량 관측소 정보 API 호출
def get_rainfall_obs_from_api():
    url = f"https://api.hrfco.go.kr/{APP_ID}/rainfall/info.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return [item for item in response.json().get("content", []) if "서울" in item.get("addr", "")]
    except requests.RequestException as e:
        print(f"API 요청 오류: {e}")
        return []

# 강수량 실시간 데이터 API 호출
def get_rainfall_current_from_api():
    url = f"https://api.hrfco.go.kr/{APP_ID}/rainfall/list/10M.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("content", [])
    except requests.RequestException as e:
        print(f"API 요청 오류: {e}")
        return []

# 수위 관측소 정보 API 호출
def get_waterlevel_obs_from_api():
    url = f"https://api.hrfco.go.kr/{APP_ID}/waterlevel/info.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return [item for item in response.json().get("content", []) if "서울" in item.get("addr", "")]
    except requests.RequestException as e:
        print(f"API 요청 오류: {e}")
        return []

# 수위 실시간 데이터 API 호출
def get_waterlevel_current_from_api():
    url = f"https://api.hrfco.go.kr/{APP_ID}/waterlevel/list/10M.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("content", [])
    except requests.RequestException as e:
        print(f"API 요청 오류: {e}")
        return []

# ✅ 강수량 관측소 정보 삽입 (부모 테이블)
def insert_rainfall_obs(cursor, data):
    sql = """
    MERGE INTO rainfall_info A
    USING (SELECT :1 AS rf_obs_cd, :2 AS obs_nm, :3 AS agc_nm, :4 AS addr, :5 AS etc_addr, :6 AS lon, :7 AS lat FROM dual) B
    ON (A.rf_obs_cd = B.rf_obs_cd)
    WHEN MATCHED THEN UPDATE SET
        A.obs_nm = B.obs_nm, A.agc_nm = B.agc_nm, A.addr = B.addr, A.etc_addr = B.etc_addr, A.lon = B.lon, A.lat = B.lat
    WHEN NOT MATCHED THEN
        INSERT (rf_obs_cd, obs_nm, agc_nm, addr, etc_addr, lon, lat) 
        VALUES (B.rf_obs_cd, B.obs_nm, B.agc_nm, B.addr, B.etc_addr, B.lon, B.lat)
    """
    insert_data = [(item["rfobscd"], item["obsnm"], item["agcnm"], item["addr"], item["etcaddr"], item["lon"], item["lat"]) for item in data]
    if insert_data:
        cursor.executemany(sql, insert_data)
        print(f"{len(insert_data)}개의 강수량 관측소 데이터 삽입 완료")

# ✅ 수위 관측소 정보 삽입 (부모 테이블)
def insert_waterlevel_obs(cursor, data):
    sql = """
    MERGE INTO waterlevel_info A
    USING (SELECT :1 AS wl_obs_cd, :2 AS obs_nm, :3 AS agc_nm, :4 AS addr, :5 AS etc_addr, :6 AS lon, :7 AS lat FROM dual) B
    ON (A.wl_obs_cd = B.wl_obs_cd)
    WHEN MATCHED THEN UPDATE SET
        A.obs_nm = B.obs_nm, A.agc_nm = B.agc_nm, A.addr = B.addr, A.etc_addr = B.etc_addr, A.lon = B.lon, A.lat = B.lat
    WHEN NOT MATCHED THEN
        INSERT (wl_obs_cd, obs_nm, agc_nm, addr, etc_addr, lon, lat) 
        VALUES (B.wl_obs_cd, B.obs_nm, B.agc_nm, B.addr, B.etc_addr, B.lon, B.lat)
    """
    insert_data = [(item["wlobscd"], item["obsnm"], item["agcnm"], item["addr"], item["etcaddr"], item["lon"], item["lat"]) for item in data]
    if insert_data:
        cursor.executemany(sql, insert_data)
        print(f"{len(insert_data)}개의 수위 관측소 데이터 삽입 완료")

# ✅ 강수량 실시간 데이터 삽입 (자식 테이블)
def insert_rainfall_current(cursor, data):
    sql = """
    MERGE INTO rainfall_current A
    USING (SELECT :1 AS rf_obs_cd, :2 AS mea_dt, :3 AS rf FROM dual) B
    ON (A.rf_obs_cd = B.rf_obs_cd AND A.mea_dt = B.mea_dt)
    WHEN MATCHED THEN UPDATE SET A.rf = B.rf
    WHEN NOT MATCHED THEN INSERT (rf_obs_cd, mea_dt, rf) VALUES (B.rf_obs_cd, B.mea_dt, B.rf)
    """
    insert_data = [(item["rfobscd"], item["ymdhm"], item["rf"]) for item in data]
    if insert_data:
        cursor.executemany(sql, insert_data)
        print(f"{len(insert_data)}개의 강수량 실시간 데이터 삽입 완료")

# ✅ 수위 실시간 데이터 삽입 (자식 테이블)
def insert_waterlevel_current(cursor, data):
    sql = """
    MERGE INTO waterlevel_current A
    USING (SELECT :1 AS wl_obs_cd, :2 AS mea_dt, :3 AS fw, :4 AS wl FROM dual) B
    ON (A.wl_obs_cd = B.wl_obs_cd AND A.mea_dt = B.mea_dt)
    WHEN MATCHED THEN UPDATE SET A.fw = B.fw, A.wl = B.wl
    WHEN NOT MATCHED THEN INSERT (wl_obs_cd, mea_dt, fw, wl) VALUES (B.wl_obs_cd, B.mea_dt, B.fw, B.wl)
    """
    insert_data = [(item["wlobscd"], item["ymdhm"], item["fw"], item["wl"]) for item in data]
    if insert_data:
        cursor.executemany(sql, insert_data)
        print(f"{len(insert_data)}개의 수위 실시간 데이터 삽입 완료")

# ✅ 전체 업데이트 로직
def scheduled_task():
    print("스케줄러 실행: 데이터 업데이트 시작")
    connection = get_db_connection()
    if connection is None:
        print("DB 연결 실패")
        return

    cursor = connection.cursor()
    try:
        insert_rainfall_obs(cursor, get_rainfall_obs_from_api())
        insert_waterlevel_obs(cursor, get_waterlevel_obs_from_api())
        insert_rainfall_current(cursor, get_rainfall_current_from_api())
        insert_waterlevel_current(cursor, get_waterlevel_current_from_api())

        connection.commit()
        print("데이터 업데이트 완료")

    except Exception as e:
        print(f"오류 발생: {e}")
        connection.rollback()

    finally:
        cursor.close()
        connection.close()

print("프로그램 시작: 초기 데이터 업데이트 실행")
scheduled_task()

scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, "interval", minutes=1)
scheduler.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("프로그램 종료")
    scheduler.shutdown()
