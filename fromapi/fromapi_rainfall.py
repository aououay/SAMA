from datetime import datetime, timedelta #날짜, 시간
from apscheduler.schedulers.background import BackgroundScheduler #백그라운드 실핼 스케줄러
import time #무한 루프 유지
import oracledb
import requests #HTTP(API) 요청을 보내 데이터 가져옴

# Oracle DB 연결 정보
DB_CONFIG = {
    "user": "SAMA",
    "password": "SAMA1234",
    "dsn": "192.168.100.30:1522/XE" #dsn(Data Source Name)은 Oracle 데이터베이스의 접속 정보
}

# DB 연결 함수
def get_db_connection():
    try:
        connection = oracledb.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        return None

def round_down_to_nearest_10_minutes(dt):
    """ 현재 시간을 10분 단위로 내림 (예: 23:16 → 23:10)
     dt.minute // 10 : 현재 시간(분)을 10으로 나눈 몫
     """
    return dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)

# 실시간 강우량 정보 API
def get_rf_current_fromapi(rfobscd):

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'

    # 현재 시간 및 2일 전 시간 계산
    #API 요청 시 분 단위가 10, 20, 30, 40, 50, 60(00)으로 맞춤
    now = datetime.now() #현재시간
    now_rounded = now.replace(minute=0, second=0, microsecond=0)
    one_day_ago = now_rounded - timedelta(days=2) #2일전 데이터

    # 날짜를 'yyyyMMddHHmm' 형식으로 변환
    sdt = one_day_ago.strftime('%Y%m%d%H%M')
    edt = now_rounded.strftime('%Y%m%d%H%M')

    url = f"https://api.hrfco.go.kr/{appid}/rainfall/list/1H/{rfobscd}/{sdt}/{edt}.json"
    print(f'요청 url: {url}')
    try:
        # API 요청
        response = requests.get(url)

        # 요청 실패 시 예외 처리
        if response.status_code != 200:
            print(f"API 요청 실패 ({rfobscd}): {response.status_code}")
            return []
        # 응답 데이터 JSON 형식으로 변환
        return response.json().get('content', [])

    except Exception as e:
        print(f"API 요청 중 오류 발생 ({rfobscd}): {str(e)}")
        return []

# 서울 지역 관측소 리스트
seoul_rfobs_list = ["10184070","10184080","10184100","10184140","10184190","10184200","10194030"]

#서울 지역 관측소 데이터를 딕셔너리에 저장
all_data = {}
for rfobscd in seoul_rfobs_list:
    all_data[rfobscd] = get_rf_current_fromapi(rfobscd)

# 강우량 관측소 정보 API
def get_rainfall_fromapi():

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'

    url = f"https://api.hrfco.go.kr/{appid}/rainfall/info.json"
    print(f'요청 url: {url}')
    # 응답 데이터 JSON 형식으로 저장
    data = requests.get(url).json()['content']

    # "addr" 필드에 "서울"이 포함된 데이터만 필터링
    filtered_data = [item for item in data if "서울" in item.get("addr", "")]
    return filtered_data

# 관측소 정보 insert
def insert_rainfall_obs():
    conn = get_db_connection()
    if conn is None:
        print("DB 연결 실패")
        return

    # cursor를 통해 SQL 실행
    cursor = conn.cursor()



    # Merge into 이용해서 기존 관측소코드 데이터가 있으면 업데이트 없으면 삽입
    try:
        data = get_rainfall_fromapi()
        insert_sql = '''
                MERGE INTO rainfall_info A
                USING (SELECT :1 AS rf_obs_cd, 
                              :2 AS obs_nm, 
                              :3 AS agc_nm, 
                              :4 AS addr, 
                              :5 AS etc_addr, 
                              :6 AS lon, 
                              :7 AS lat 
                       FROM dual) B
                ON (A.rf_obs_cd = B.rf_obs_cd)
                WHEN MATCHED THEN
                    UPDATE SET 
                        A.obs_nm = B.obs_nm,
                        A.agc_nm = B.agc_nm,
                        A.addr = B.addr,
                        A.etc_addr = B.etc_addr,
                        A.lon = B.lon,
                        A.lat = B.lat
                WHEN NOT MATCHED THEN
                    INSERT (rf_obs_cd, obs_nm, agc_nm, addr, etc_addr, lon, lat)
                    VALUES (B.rf_obs_cd, B.obs_nm, B.agc_nm, B.addr, B.etc_addr, B.lon, B.lat)        
            '''

        #데이터 변환 리스트
        insert_data = []
        for item in data:
            try:
                insert_data.append((
                    safe_float(item["rfobscd"]),  # 숫자 변환
                    item["obsnm"],
                    item["agcnm"],
                    item["addr"],
                    item["etcaddr"],
                    dms_to_decimal(item["lon"]),  # DMS → 십진수 변환
                    dms_to_decimal(item["lat"])  # DMS → 십진수 변환
                ))
            except ValueError as e:
                print(f"데이터 변환 오류 발생: {e} (데이터: {item})")

        success_count = 0  # 성공한 데이터 개수
        failed_data = []  # 실패한 데이터 저장용 리스트

        # 데이터가 있을 때만 INSERT 실행
        if insert_data:
            for row in insert_data:
                try:
                    print(f"INSERT 시도: {row}")  # 삽입 전 데이터 확인
                    cursor.execute(insert_sql, row)  # 개별 실행
                    success_count += 1
                except oracledb.IntegrityError as e:
                    print(f'무결성 제약 조건 오류:{e}')
                    failed_data.append(row)
                except Exception as e:
                    print(f'SQL 실행 오류:{e} (데이터: {row})')
                    failed_data.append(row)

            conn.commit()
            print(f"{success_count}개의 데이터 삽입 완료!")
            msg = "success"

        # INSERT 실패 데이터 출력
        if failed_data:
            print(f"삽입 실패한 데이터 ({len(failed_data)}개):")
            for failed_row in failed_data:
                print(f"   실패: {failed_row}")
            msg = "fail"

            return msg

    except Exception as e:
        print(f"오류 발생: {e}")
        conn.rollback()  # 오류 발생 시 롤백

    finally:
        if cursor:
            cursor.close() #커서 닫기
            print("cursor.close()!")
        if conn:
            conn.close()
            print("conn.close()!") # DB 연결 닫기


def insert_rainfall_current():

    conn = get_db_connection()
    if conn is None:
        print("DB 연결 실패")
        return
    cursor = conn.cursor()

    #관측소코드, 측정시간이 중복되지 않으면 삽입, 있으면 업데이트
    try:
        insert_sql = '''
            MERGE INTO rainfall_current A
            USING (SELECT :1 AS rf_obs_cd, :2 AS mea_dt, :3 AS rf FROM dual) B
            ON (A.rf_obs_cd = B.rf_obs_cd AND A.mea_dt = B.mea_dt)
            WHEN NOT MATCHED THEN
                INSERT (rf_obs_cd, mea_dt, rf, reg_dt) 
                VALUES (B.rf_obs_cd, B.mea_dt, B.rf, sysdate)
            '''

        insert_data = []
        for rfobscd in seoul_rfobs_list:
            data = get_rf_current_fromapi(rfobscd)
            for item in data:
                print(f"원본 데이터: {item}")
                try:
                    insert_data.append((
                        safe_float(item["rfobscd"]),
                        item["ymdhm"],
                        safe_float(item["rf"])
                    ))
                except ValueError as e:
                    print(f"데이터 변환 오류 발생: {e} (데이터: {item})")

        success_count = 0  # 성공한 데이터 개수
        failed_data = []  # 실패한 데이터 저장용 리스트

        # 데이터가 있을 때만 INSERT 실행(불필요한 데이터 삽입 방지)
        if insert_data:
            try:
                print(f"{len(insert_data)}개 데이터 삽입 시도")
                cursor.executemany(insert_sql, insert_data)
                conn.commit()
                print(f"{len(insert_data)}개의 데이터 삽입 완료!")
            except oracledb.IntegrityError as e:
                print(f'무결성 제약 조건 오류: {e}')
                conn.rollback()
            except Exception as e:
                print(f'SQL 실행 오류: {e}')
                conn.rollback()

        # INSERT 실패 데이터 출력
        if failed_data:
            print(f"삽입 실패한 데이터 ({len(failed_data)}개):")
            for failed_row in failed_data:
                print(f"   실패: {failed_row}")

    except Exception as e:
        print(f"오류 발생: {e}")
        conn.rollback()

    finally:
        if cursor:
            cursor.close()  # 커서 닫기
            print("cursor.close()!")
        if conn:
            conn.close()
            print("conn.close()!")  # DB 연결 닫기


# DMS(도-분-초) → 십진수 변환 함수
def dms_to_decimal(dms_str):
    """DMS (127-02-00) 형식을 십진수 (127.xxxx) 형식으로 변환"""
    if not dms_str or dms_str.strip() == "":  # 빈 값 체크
        return 0.0  # 빈 값이면 0 반환

    parts = dms_str.strip().split('-')  # '-'로 분리 및 공백 제거
    if len(parts) != 3:
        raise ValueError(f"DMS 형식이 잘못되었습니다: {dms_str} (예: 126-54-52)")

    degrees = int(parts[0])  # 도 (Degrees)
    minutes = int(parts[1])  # 분 (Minutes)
    seconds = float(parts[2])  # 초 (Seconds)

    # 공식 적용
    decimal_degrees = degrees + (minutes / 60) + (seconds / 3600)

    return round(decimal_degrees, 6)  # 소수점 6자리까지 반올림

# 숫자 변환 함수 (빈 값 처리 포함)
def safe_float(value):
    """값이 비어있거나 None이면 0.0으로 변환"""
    try:
        return float(value) if value not in [None, "", " "] else 0.0
    except ValueError:
        return 0.0


# 전체 업데이트 함수 (관측소 → 실시간 데이터 순서)
def scheduled_task():
    print("스케줄러 실행: 데이터 업데이트 시작")

    try:
        # 부모 데이터(관측소 정보) 먼저 업데이트
        insert_rainfall_obs()

        insert_rainfall_current()

        # 자식 데이터(실시간 데이터) 업데이트
        connection = get_db_connection()
        if connection is None:
            print("DB 연결 실패")
            return

        cursor = connection.cursor()

        connection.commit()
        print("데이터 업데이트 완료")
    except Exception as e:
        print(f"오류 발생: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# 프로그램 시작 시 초기 실행
print("프로그램 시작: 초기 데이터 업데이트 실행")
scheduled_task()

# 스케줄러 설정
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, "cron", minute=7)
scheduler.start()

# 프로그램 실행 유지
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("프로그램 종료")
    scheduler.shutdown()