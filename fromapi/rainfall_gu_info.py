import oracledb
import time
from apscheduler.schedulers.background import BackgroundScheduler

# Oracle DB 연결 정보
DB_CONFIG = {
    "user": "SAMA",
    "password": "SAMA1234",
    "dsn": "192.168.100.30:1522/XE"
}


# DB 연결 함수
def get_db_connection():
    try:
        connection = oracledb.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        print(f"DB 연결 오류:{e}")
        return None

# 관측소 코드별 강우량 검색 함수
def get_rainfall_by_obs_cd(rf_obs_cd):
    conn = get_db_connection()
    if conn is None:
        return None

    cursor = conn.cursor()
    sql = """
        SELECT rf_obs_cd, mea_dt, rf, reg_dt, district
        FROM rainfall_info
        WHERE rf_obs_cd = :rf_obs_cd
        ORDER BY mea_dt DESC
    """

    cursor.execute(sql, {"rf_obs_cd": rf_obs_cd})
    data = cursor.fetchall()

    cursor.close()
    conn.close()

    return data


# GU_INFO 테이블에서 자치구 정보 가져오기
def select_gu_info(cursor):
    '''
    GU_INFO 테이블에서 구코드, 위도, 경도 정보를 가져오는 쿼리 메서드
    :param cursor:
    :return: gu_info (튜플이 담겨져 있는 리스트)
    '''
    sql = "SELECT GU_CD, MS_LAT, MS_LON FROM GU_INFO"
    cursor.execute(sql)
    return cursor.fetchall()


# RAINFALL_INFO 테이블에서 강우량 관측소 정보 가져오기
def select_rainfall_info(cursor):
    '''
    RAINFALL_INFO 테이블에서 강수량 관측소 코드, 위도, 경도 정보를 가져오는 쿼리 메서드
    :param cursor:
    :return: rainfall_info (튜플이 담겨져 있는 리스트)
    '''
    sql = "SELECT RF_OBS_CD, LAT, LON FROM rainfall_info"
    cursor.execute(sql)
    return cursor.fetchall()

# 자치구 - 강우량 관측소 매핑 및 INSERT
def insert_rf_obs_gu_info(cursor, gu_info, rainfall_info):
    '''
    GU_INFO와 RAINFALL_INFO 테이블의 데이터를 매핑하여 RF_OBS_GU_INFO 테이블에 삽입하는 메서드
    :param cursor:
    :param gu_info: GU_INFO에서 가져온 데이터 (구코드, 위도, 경도)
    :param rainfall_info: RAINFALL_INFO에서 가져온 데이터 (강수량 관측소 코드, 위도, 경도)
    :return: 없음
    '''
    # 이미 매핑된 GU_CD를 저장할 집합
    mapped_gu_cd = set()

    # GU_CD와 RF_OBS_CD 매핑
    for gu in gu_info:
        gu_cd, gu_lat, gu_lon = gu

        # 이미 매핑된 GU_CD라면 건너뜀
        if gu_cd in mapped_gu_cd:
            continue

        for rainfall in rainfall_info:
            rf_obs_cd, rf_lat, rf_lon = rainfall
            if is_close(gu_lat, gu_lon, rf_lat, rf_lon): # is_close: 두 지점이 가까운지 판별하는 함수 -> 0.1 이내면 매핑
                # RF_OBS_GU_INFO 테이블에 매핑 정보 삽입
                sql = """
                MERGE INTO RF_OBS_GU_INFO A
                USING (SELECT :gu_cd AS GU_CD, :rf_obs_cd AS RF_OBS_CD FROM DUAL) B
                ON (A.GU_CD = B.GU_CD AND A.RF_OBS_CD = B.RF_OBS_CD)
                WHEN NOT MATCHED THEN
                    INSERT (GU_CD, RF_OBS_CD)
                    VALUES (B.GU_CD, B.RF_OBS_CD)
                """
                cursor.execute(sql, {"gu_cd": gu_cd, "rf_obs_cd": rf_obs_cd})
                mapped_gu_cd.add(gu_cd)
                break  # 한 개의 RF_OBS_CD만 매핑하고 다음 자치구로 넘어감


def is_close(lat1, lon1, lat2, lon2, threshold=0.1):
    '''
    두 위치가 충분히 가까운지를 판단하는 함수 (단위: 위도/경도 차이)
    :param lat1, lon1: 첫 번째 위치 (위도, 경도)
    :param lat2, lon2: 두 번째 위치 (위도, 경도)
    :param threshold: 위치가 얼마나 가까운지를 판단하는 기준 (기본값: 0.1)
    :return: 가까우면 True, 그렇지 않으면 False
    '''
    # 위도와 경도의 차이를 계산하여 threshold 값 이내면 True
    return abs(lat1 - lat2) < threshold and abs(lon1 - lon2) < threshold

# GU_INFO <-> RAINFALL_INFO 매핑 실행
def update_rf_obs_gu_info():
    conn = get_db_connection()
    if conn is None:
        return

    cursor = conn.cursor()
    try:
        gu_info = select_gu_info(cursor)
        rainfall_info = select_rainfall_info(cursor)
        insert_rf_obs_gu_info(cursor, gu_info, rainfall_info)
        conn.commit()
    except Exception as e:
        print(f"오류 발생: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# 스케줄링할 전체 업데이트 함수
def scheduled_task():
    print("스케줄러 실행: 데이터 업데이트 시작")

    try:
        update_rf_obs_gu_info()
        print("자치구-관측소 매핑 완료")
    except Exception as e:
        print(f"오류 발생: {e}")


# 프로그램 시작 시 초기 실행
print("프로그램 시작: 초기 데이터 업데이트 실행")
scheduled_task()

# 스케줄러 설정 (매일 0시 10분 실행)
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, "cron", hour=0, minute=10)
#scheduler.add_job(scheduled_task, "interval", minutes=1)
scheduler.start()

# 프로그램 실행 유지
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("프로그램 종료")
    scheduler.shutdown()