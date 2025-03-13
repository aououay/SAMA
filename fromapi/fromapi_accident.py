import time
import oracledb
import requests
from apscheduler.schedulers.background import BackgroundScheduler

# Oracle DB 연결 정보
DB_CONFIG = {
    "user": "system",
    "password": "pass",
    "dsn": "localhost:1521/XE"
}

# OpenAPI 키
APP_ID = "6d51755a53616f7536366f775a526e"


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


def get_accident_stats_from_api():
    """
    OpenAPI에서 사고 통계 데이터를 가져오는 함수
    """
    url = f"http://openapi.seoul.go.kr:8088/{APP_ID}/json/weekDisasterStat/1/1000/"
    print(f"url: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("weekDisasterStat", {}).get("row", [])
        return data
    except requests.RequestException as e:
        print(f"API 요청 오류: {e}")
        return []


def insert_accident_stats_data(cursor, data):
    """
    사고 통계 데이터를 accident_stats 테이블에 삽입하는 함수
    """
    sql = """
    MERGE INTO accident_stats A
    USING (SELECT :1 AS acdnt_cnt, :2 AS dcsd_cnt, :3 AS stats_crtr_sta_dt, :4 AS stats_crtr_end_dt FROM dual) B
    ON (A.stats_crtr_sta_dt = B.stats_crtr_sta_dt AND A.stats_crtr_end_dt = B.stats_crtr_end_dt)
    WHEN MATCHED THEN UPDATE SET A.acdnt_cnt = B.acdnt_cnt, A.dcsd_cnt = B.dcsd_cnt
    WHEN NOT MATCHED THEN INSERT (acdnt_cnt, dcsd_cnt, stats_crtr_sta_dt, stats_crtr_end_dt)
    VALUES (B.acdnt_cnt, B.dcsd_cnt, B.stats_crtr_sta_dt, B.stats_crtr_end_dt)
    """

    insert_data = []
    for item in data:
        try:
            # 날짜 데이터 가공
            start_date, end_date = None, None
            if "STATS_CRTR_YMD" in item and isinstance(item["STATS_CRTR_YMD"], str) and "~" in item["STATS_CRTR_YMD"]:
                dates = item["STATS_CRTR_YMD"].split("~")
                start_date, end_date = dates[0].strip(), dates[1].strip()

            insert_data.append((
                int(item.get("ACDNT_NOCS", 0)),  # 사고 건수
                int(item.get("DCSD_CNT", 0)),  # 사망자 수
                start_date,  # 통계 시작일
                end_date  # 통계 종료일
            ))
        except ValueError as e:
            print(f"데이터 변환 오류 발생: {e} (데이터: {item})")

    if insert_data:
        cursor.executemany(sql, insert_data)
        print(f"{len(insert_data)}개의 사고 통계 데이터 삽입 완료")
        print(f"DB 업데이트 실행됨: {cursor.rowcount}개 행 변경됨")


def scheduled_task():
    """
    스케줄러에서 실행할 작업 함수 (사고 통계 업데이트)
    """
    print("스케줄러 실행: 사고 통계 데이터 업데이트 시작")

    connection = get_db_connection()
    if connection is None:
        print("DB 연결 실패")
        return

    cursor = connection.cursor()

    try:
        # OpenAPI에서 사고 통계 데이터 가져오기
        accident_stats_data = get_accident_stats_from_api()

        # DB에 데이터 삽입
        insert_accident_stats_data(cursor, accident_stats_data)

        connection.commit()
        print("사고 통계 데이터 업데이트 완료")

    except Exception as e:
        print(f"오류 발생: {e}")
        connection.rollback()

    finally:
        cursor.close()
        connection.close()


# 스케줄러 설정 (하루에 한 번 실행 - 매일 00:00)
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, "interval", minutes=60)
scheduler.start()

# 프로그램 실행 중지 방지 (무한 루프 실행)
if __name__ == "__main__":
    try:
        print("프로그램 시작됨")  # 프로그램이 실행되는지 확인
        scheduled_task()  # 스케줄러 실행 전에 직접 실행
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("프로그램 종료")
        scheduler.shutdown()
