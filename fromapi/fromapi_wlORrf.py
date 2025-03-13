from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import time
import oracledb, requests
from sqlalchemy.orm import Session

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
        return None

def round_down_to_nearest_10_minutes(dt):
    """ 현재 시간을 10분 단위로 내림 (예: 23:16 → 23:10) """
    return dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)

def get_wl_current_fromapi(wlobscd):

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'

    # 현재 시간 및 1개월 전 시간 계산
    #API 요청 시 분 단위가 10, 20, 30, 40, 50, 60(00)으로 맞춤
    now = datetime.now()
    now_rounded = round_down_to_nearest_10_minutes(now)
    one_month_ago = now_rounded - timedelta(days=1)

    # 날짜를 'yyyyMMddHHmm' 형식으로 변환
    sdt = one_month_ago.strftime('%Y%m%d%H%M')
    edt = now_rounded.strftime('%Y%m%d%H%M')

    # URL 생성
    url = f"https://api.hrfco.go.kr/{appid}/waterlevel/list/10M/{wlobscd}/{sdt}/{edt}.json"
    print(f'요청 url: {url}')
    try:
        # API 요청
        response = requests.get(url)

        # 요청 실패 시 예외 처리
        if response.status_code != 200:
            return []
        # 응답 데이터
        data = response.json().get('content', [])

        filtered_data = [item for item in data if item.get("wlobscd") in seoul_wlobs_list]
        return filtered_data
    except Exception as e:
        return []

seoul_wlobs_list = ["1018640","1018655","1018658","1018662"
                   ,"1018669","1018670","1018675","1018680","1018683"
                   ,"1018686","1018692","1018697","1018698"
                    ]
all_data = {}
for wlobscd in seoul_wlobs_list:
    all_data[wlobscd] = get_wl_current_fromapi(wlobscd)


def get_rf_current_fromapi(rfobscd):

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'

    # 현재 시간 및 1개월 전 시간 계산
    #API 요청 시 분 단위가 10, 20, 30, 40, 50, 60(00)으로 맞춤
    now = datetime.now()
    now_rounded = round_down_to_nearest_10_minutes(now)
    one_day_ago = now_rounded - timedelta(days=1)

    # 날짜를 'yyyyMMddHHmm' 형식으로 변환
    sdt = one_day_ago.strftime('%Y%m%d%H%M')
    edt = now_rounded.strftime('%Y%m%d%H%M')

    url = f"https://api.hrfco.go.kr/{appid}/rainfall/list/10M/{rfobscd}/{sdt}/{edt}.json"
    print(f'요청 url: {url}')
    try:
        # API 요청
        response = requests.get(url)

        # 요청 실패 시 예외 처리
        if response.status_code != 200:
            print(f"API 요청 실패 ({rfobscd}): {response.status_code}")
            return []
        # 응답 데이터
        return response.json().get('content', [])

    except Exception as e:
        print(f"API 요청 중 오류 발생 ({rfobscd}): {str(e)}")
        return []

seoul_rfobs_list = ["10184070","10184080","10184100","10184140","10184190","10184200","10194030"]

all_data = {}
for rfobscd in seoul_rfobs_list:
    all_data[rfobscd] = get_rf_current_fromapi(rfobscd)


def get_rainfall_fromapi():

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'

    url = f"https://api.hrfco.go.kr/{appid}/rainfall/info.json"
    print(f'요청 url: {url}')
    # 응답 데이터
    data = requests.get(url).json()['content']

    # "addr" 필드에 "서울"이 포함된 데이터만 필터링
    filtered_data = [item for item in data if "서울" in item.get("addr", "")]
    return filtered_data


def get_wl_obs_fromapi():

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'
    url = f"https://api.hrfco.go.kr/{appid}/waterlevel/info.json"
    print(f'요청 url: {url}')
    # 응답 데이터
    data = requests.get(url).json()['content']

    # "addr" 필드에 "서울"이 포함된 데이터만 필터링
    filtered_data = [item for item in data if "서울" in item.get("addr", "")]
    return filtered_data

def insert_openapi_wl_obs():
    conn = get_db_connection()
    cursor = conn.cursor()

    # opanpi 조회
    data = get_wl_obs_fromapi()

    insert_sql = """
            MERGE INTO waterlevel_info A
            USING (SELECT :1 AS wl_obs_cd, :2 AS obs_nm, :3 AS agc_nm, :4 AS addr, :5 AS etc_addr,
                          :6 AS lon, :7 AS lat, :8 AS gdt, :9 AS att_wl, :10 AS wrn_wl,
                          :11 AS alm_wl, :12 AS srs_wl, :13 AS pfh, :14 AS fstn_yn 
                   FROM dual) B
            ON (A.wl_obs_cd = B.wl_obs_cd)
            WHEN MATCHED THEN
                UPDATE SET 
                    A.obs_nm = B.obs_nm,
                    A.agc_nm = B.agc_nm,
                    A.addr = B.addr,
                    A.etc_addr = B.etc_addr,
                    A.lon = B.lon,
                    A.lat = B.lat,
                    A.gdt = B.gdt,
                    A.att_wl = B.att_wl,
                    A.wrn_wl = B.wrn_wl,
                    A.alm_wl = B.alm_wl,
                    A.srs_wl = B.srs_wl,
                    A.pfh = B.pfh,
                    A.fstn_yn = B.fstn_yn
            WHEN NOT MATCHED THEN
                INSERT (wl_obs_cd, obs_nm, agc_nm, addr, etc_addr, lon, lat, gdt, att_wl, wrn_wl, alm_wl, srs_wl, pfh, fstn_yn)
                VALUES (B.wl_obs_cd, B.obs_nm, B.agc_nm, B.addr, B.etc_addr, B.lon, B.lat, B.gdt, B.att_wl, B.wrn_wl, B.alm_wl, B.srs_wl, B.pfh, B.fstn_yn)
            """

    insert_data = []
    for item in data:
        try:
            insert_data.append((
                safe_float(item["wlobscd"]),  # 숫자 변환
                item["obsnm"],
                item["agcnm"],
                item["addr"],
                item["etcaddr"],
                dms_to_decimal(item["lon"]),  # DMS → 십진수 변환
                dms_to_decimal(item["lat"]),  # DMS → 십진수 변환
                safe_float(item["gdt"]),  # 숫자 변환
                safe_float(item["attwl"]),  # 숫자 변환
                safe_float(item["wrnwl"]),  # 숫자 변환
                safe_float(item["almwl"]),  # 숫자 변환
                safe_float(item["srswl"]),  # 숫자 변환
                safe_float(item["pfh"]),  # 숫자 변환
                item["fstnyn"]
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
                failed_data.append(row)
            except Exception as e:
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



def insert_rainfall_obs():
    conn = get_db_connection()
    if conn is None:
        print("DB 연결 실패")
        return
    cursor = conn.cursor()

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

        return msg;

def insert_waterlevel_current():

    conn = get_db_connection()

    cursor = conn.cursor()

    insert_sql = '''
        MERGE INTO waterlevel_current A
        USING (
            SELECT :1 AS wl_obs_cd, :2 AS mea_dt, :3 AS wl, :4 AS fw FROM dual
        ) B
        ON (A.wl_obs_cd = B.wl_obs_cd AND A.mea_dt = B.mea_dt)
        WHEN MATCHED THEN  
            UPDATE SET A.wl = CASE WHEN A.wl = 0.0 AND B.wl <> 0.0 THEN B.wl ELSE A.wl END,
                       A.fw = CASE WHEN A.wl = 0.0 AND B.wl <> 0.0 THEN B.fw ELSE A.fw END
        WHERE A.wl = 0.0 
        WHEN NOT MATCHED THEN  
            INSERT (wl_obs_cd, mea_dt, wl, fw, reg_dt) 
            VALUES (B.wl_obs_cd, B.mea_dt, B.wl, B.fw, sysdate)
        WHERE B.wl <> 0.0
        '''

    insert_data = []
    for wlobscd in seoul_wlobs_list:
        data = get_wl_current_fromapi(wlobscd)
        for item in data:
            try:
                insert_data.append((
                    safe_float(item["wlobscd"]),
                    item["ymdhm"],
                    safe_float(item["wl"] if item["wl"] not in [None, "", " "] else None),
                    safe_float(item["fw"] if item["fw"] not in [None, "", " "] else None)

                ))

            except ValueError as e:
                print(f"데이터 변환 오류 발생: {e} (데이터: {item})")

    success_count = 0  # 성공한 데이터 개수
    failed_data = []  # 실패한 데이터 저장용 리스트

    # 데이터가 있을 때만 INSERT 실행
    if insert_data:
        try:
            print(f"{len(insert_data)}개 데이터 삽입 시도")
            cursor.executemany(insert_sql, insert_data)  # ✅ 4개의 바인딩 변수로 맞춤
            conn.commit()
            print(f"{len(insert_data)}개의 데이터 삽입 완료!")
        except oracledb.IntegrityError as e:
            print(f'무결성 제약 조건 오류: {e}')
        except Exception as e:
            print(f'SQL 실행 오류: {e}')

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

def insert_rainfall_current():

    conn = get_db_connection()
    if conn is None:
        print("DB 연결 실패")
        return
    cursor = conn.cursor()

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

    # 데이터가 있을 때만 INSERT 실행
    if insert_data:
        try:
            print(f"{len(insert_data)}개 데이터 삽입 시도")
            cursor.executemany(insert_sql, insert_data)
            conn.commit()
            print(f"{len(insert_data)}개의 데이터 삽입 완료!")
        except oracledb.IntegrityError as e:
            print(f'무결성 제약 조건 오류: {e}')
        except Exception as e:
            print(f'SQL 실행 오류: {e}')

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
        insert_openapi_wl_obs()

        insert_rainfall_current()
        insert_waterlevel_current()

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
#scheduler.add_job(scheduled_task, "interval", minutes=10, max_instances=4)
scheduler.add_job(scheduled_task, "cron", minute="7,17,27,37,47,57", max_instances=4)
#scheduler.add_job(scheduled_task, "cron", hour=0, minute=10)
scheduler.start()

# 프로그램 실행 유지
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("프로그램 종료")
    scheduler.shutdown()