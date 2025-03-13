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
    """
    데이터베이스 연결을 생성하는 함수
    """
    try:
        return oracledb.connect(**DB_CONFIG)
    except Exception as e:
        print(f"DB 연결 오류: {e}")
        return None


def get_rainfall_from_api():
    """
    OpenAPI에서 강수량 데이터를 가져오는 함수
    """
    url = f"https://api.hrfco.go.kr/{APP_ID}/rainfall/list/10M.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("content", [])
        return data
    except requests.RequestException as e:
        print(f"API 요청 오류: {e}")
        return []


def get_waterlevel_from_api():
    """
    OpenAPI에서 수위 데이터를 가져오는 함수
    """
    url = f"https://api.hrfco.go.kr/{APP_ID}/waterlevel/list/10M.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("content", [])
        return data
    except requests.RequestException as e:
        print(f"API 요청 오류: {e}")
        return []


def insert_rainfall_data(cursor, data):
    """
    강수량 데이터를 rainfall_current 테이블에 삽입하는 함수
    """
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
        print(f"{len(insert_data)}개의 강수량 데이터 삽입 완료")


def insert_waterlevel_data(cursor, data):
    """
    수위 데이터를 waterlevel_current 테이블에 삽입하는 함수
    """
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
        print(f"{len(insert_data)}개의 수위 데이터 삽입 완료")


def scheduled_task():
    """
    스케줄러에서 실행할 작업 함수
    """
    print("스케줄러 실행: 데이터 업데이트 시작")

    connection = get_db_connection()
    if connection is None:
        print("DB 연결 실패")
        return

    cursor = connection.cursor()

    try:
        # OpenAPI에서 데이터 가져오기
        rainfall_data = get_rainfall_from_api()
        waterlevel_data = get_waterlevel_from_api()

        # DB에 데이터 삽입
        insert_rainfall_data(cursor, rainfall_data)
        insert_waterlevel_data(cursor, waterlevel_data)

        connection.commit()
        print("데이터 업데이트 완료")

    except Exception as e:
        print(f"오류 발생: {e}")
        connection.rollback()

    finally:
        cursor.close()
        connection.close()


# 스케줄러 설정 (10분마다 실행)
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, "interval", minutes=1)
scheduler.start()

# 프로그램 실행 중지 방지 (무한 루프 실행)
if __name__ == "__main__":
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("프로그램 종료")
        scheduler.shutdown()
