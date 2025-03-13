from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
import time
import oracledb, requests
from sqlalchemy.orm import Session


app = FastAPI()
# FastAPI 앱 생성


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

@app.get("/items")
async def read_items():
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM GU_INFO")  # 여기에 적절한 테이블명을 입력하세요

        # 컬럼명 가져오기
        columns = [col[0] for col in cursor.description]

        # JSON 변환 (각 행을 딕셔너리로 매핑)
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

    return data

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/insert/openapi/wlObs") #수위관측소정보
async def insert_openapi_wl_obs():
    conn = get_db_connection()
    cursor = conn.cursor()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

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

    return msg;


@app.get("/insert/openapi/acStats") #사고통계
async def insert_accident_stats():
    conn = get_db_connection()
    cursor = conn.cursor()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    # Open API 조회
    data = get_accident_stats_fromapi()

    insert_sql = """
            MERGE INTO accident_stats target
            USING (SELECT :1 AS acdnt_cnt,
                          :2 AS dcsd_cnt,
                          :3 AS stats_crtr_sta_dt,
                          :4 AS stats_crtr_end_dt
                   FROM dual) source
            ON (target.stats_crtr_sta_dt = source.stats_crtr_sta_dt
                AND target.stats_crtr_end_dt = source.stats_crtr_end_dt)
            WHEN NOT MATCHED THEN
                INSERT (acdnt_cnt, dcsd_cnt, stats_crtr_sta_dt, stats_crtr_end_dt)
                VALUES (source.acdnt_cnt, source.dcsd_cnt, source.stats_crtr_sta_dt, source.stats_crtr_end_dt)

        """
    insert_data = []

    for item in data:
        try:
            # 날짜 데이터 확인
            start_date = item.get("stats_crtr_sta_dt", None)  # 날짜 시작
            end_date = item.get("stats_crtr_end_dt", None)  # 날짜 종료

            # 숫자 변환 (safe_float 사용)
            acdnt_cnt = safe_float(item.get("acdnt_cnt", 0))  # None이면 0
            dcsd_cnt = safe_float(item.get("dcsd_cnt", 0))  # None이면 0

            if start_date is None or end_date is None:
                print(f"날짜 값 없음: {item}")  # 디버깅용
                continue

            insert_data.append((
                acdnt_cnt,
                dcsd_cnt,
                start_date,
                end_date
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


@app.get("/insert/openapi/rfObs") #강수량관측소
async def insert_rainfall_obs():
    conn = get_db_connection()

    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

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

@app.get("/insert/openapi/wlcurrent") #수위 실시간
async def insert_waterlevel_current():
    conn = get_db_connection()

    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()

    data = get_wl_current_fromapi()

    insert_sql = '''
            MERGE INTO waterlevel_current A
            USING (SELECT :1 AS wl_obs_cd, 
                          :2 AS mea_dt, 
                          :3 AS fw, 
                          :4 AS wl
                   FROM dual) B
            ON (A.wl_obs_cd = B.wl_obs_cd AND A.mea_dt = B.mea_dt) 
            WHEN MATCHED THEN
                UPDATE SET 
                    A.fw = B.fw,
                    A.wl = B.wl
            WHEN NOT MATCHED THEN
                INSERT (wl_obs_cd, mea_dt, fw, wl) 
                VALUES (B.wl_obs_cd, B.mea_dt, B.fw, B.wl)
        '''

    insert_data = []
    for item in data:
        try:
            insert_data.append((
                safe_float(item["wlobscd"]),
                item["ymdhm"],
                safe_float(item["wl"]),
                safe_float(item["fw"])
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
@app.get("/insert/openapi/rfcurrent") #강수량 실시간
async def insert_rainfall_current():

    conn = get_db_connection()

    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()

    data = get_rf_current_fromapi()

    insert_sql = '''
        INSERT INTO rainfall_current (
    rf_obs_cd,
    mea_dt,
    rf,
) VALUES ( 
           :1,
           :2,
           :3,
           :4 )

        '''

    insert_data = []
    for item in data:
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



def select_gu_info(cursor):
    '''
    GU_INFO 테이블에서 구코드, 위도, 경도 정보를 가져오는 쿼리 메서드
    :param cursor:
    :return: gu_info (튜플이 담겨져 있는 리스트)
    '''

    sql = "SELECT GU_CD, MS_LAT, MS_LON FROM GU_INFO"
    cursor.execute(sql)
    gu_info = cursor.fetchall()

    return gu_info

def select_rainfall_info(cursor):
    '''
    GU_INFO 테이블에서 구코드, 위도, 경도 정보를 가져오는 쿼리 메서드
    :param cursor:
    :return: gu_info (튜플이 담겨져 있는 리스트)
    '''

    sql = "SELECT RF_OBS_CD, LAT, LON FROM rainfall_info"
    cursor.execute(sql)
    rainfall_info = cursor.fetchall()

    return rainfall_info

def get_rf_obs_gu_info():
    conn = None
    cursor = None
    try:
        # 데이터베이스 연결
        conn = oracledb.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # SQL 쿼리 실행 (조인하여 관련 데이터 조회)
        query = """
            SELECT 
                g.GU_CD, g.GU_NAME,  -- GU_INFO 테이블의 GU 코드 및 명칭
                r.RF_OBS_CD, r.OBS_NM, r.AGC_NM, r.ADDR, r.LON, r.LAT -- RAINFALL_INFO 테이블의 강수량 관측소 정보
            FROM RF_OBS_GU_INFO rg
            JOIN GU_INFO g ON rg.GU_CD = g.GU_CD
            JOIN RAINFALL_INFO r ON rg.RF_OBS_CD = r.RF_OBS_CD
        """

        cursor.execute(query)
        results = cursor.fetchall()  # 결과 가져오기

        # 데이터 출력 (또는 JSON 변환)
        data_list = []
        for row in results:
            data_list.append({
                "gu_cd": row[0],
                "gu_name": row[1],
                "rf_obs_cd": row[2],
                "obs_nm": row[3],
                "agc_nm": row[4],
                "addr": row[5],
                "lon": row[6],
                "lat": row[7],
            })

        return data_list  # JSON 형태로 반환 가능

    except oracledb.DatabaseError as e:
        print(f"데이터베이스 오류 발생: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# 함수 실행 및 출력 테스트
if __name__ == "__main__":
    data = get_rf_obs_gu_info()
    for entry in data:
        print(entry)



def get_wl_current_fromapi():
    waterlevel_current_lst = []

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'

    url = f"https://api.hrfco.go.kr/{appid}/waterlevel/list/10M.json"

    print(f'요청url:{url}')

    # 응답 데이터
    data = requests.get(url).json()['content']

    seoul_wlobs_list = ["1018640","1018645","1018655","1018658","1018660","1018662"
                       ,"1018664","1018669","1018670","1018675","1018680","1018683"
                       ,"1018685","1018686","1018692","1018695","1018697","1018698"

                        ]
    filtered_data = [item for item in data if item.get("wlobscd") in seoul_wlobs_list]
    return filtered_data


filtered_data = get_wl_current_fromapi()
print(filtered_data)



def get_rf_current_fromapi():
    rainfall_current_lst = []

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'

    url = f"https://api.hrfco.go.kr/{appid}/rainfall/list/10M.json"

    print(f'요청url:{url}')

    # 응답 데이터
    data = requests.get(url).json()['content']

    seoul_rfobs_list = ["10184070","10184080","10184100","10184140","10184190","10184200","10194030"]
    filtered_data = [item for item in data if item.get("rfobscd") in seoul_rfobs_list]
    return filtered_data


filtered_data = get_rf_current_fromapi()
print(filtered_data)

def get_rainfall_fromapi():

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'

    url = f"https://api.hrfco.go.kr/{appid}/rainfall/info.json"

    print(f'요청url:{url}')

    # 응답 데이터
    data = requests.get(url).json()['content']

    # "addr" 필드에 "서울"이 포함된 데이터만 필터링
    filtered_data = [item for item in data if "서울" in item.get("addr", "")]
    return filtered_data


def get_accident_stats_fromapi():
    accident_stats_list = []

    appid = '6d51755a53616f7536366f775a526e'

    # Open API 요청
    url = f"http://openapi.seoul.go.kr:8088/{appid}/json/weekDisasterStat/1/1000/"
    response = requests.get(url)

    # 응답 데이터 JSON 변환
    try:
        rows = response.json()['weekDisasterStat']['row']
    except (KeyError, IndexError, ValueError):
        print("API 응답 데이터가 예상과 다릅니다.")
        return []

    # 리스트 순회하면서 변환
    for data in rows:
        accident_stats_dic = {
            'acdnt_cnt': data.get('ACDNT_NOCS'),
            'dcsd_cnt': data.get('DCSD_CNT'),
        }

        # 날짜 데이터 가공 (예: "20240201-20240207" → "20240201", "20240207")
        if 'STATS_CRTR_YMD' in data and isinstance(data['STATS_CRTR_YMD'], str) and '~' in data['STATS_CRTR_YMD']:
            dates = data['STATS_CRTR_YMD'].split('~')
            accident_stats_dic['stats_crtr_sta_dt'] = dates[0].strip()
            accident_stats_dic['stats_crtr_end_dt'] = dates[1].strip()
        else:
            accident_stats_dic['stats_crtr_sta_dt'] = None
            accident_stats_dic['stats_crtr_end_dt'] = None

        accident_stats_list.append(accident_stats_dic)

    return accident_stats_list


def get_wl_obs_fromapi():

    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'
    url = f"https://api.hrfco.go.kr/{appid}/waterlevel/info.json"

    print(f'요청url:{url}')

    # 응답 데이터
    data = requests.get(url).json()['content']

    # "addr" 필드에 "서울"이 포함된 데이터만 필터링
    filtered_data = [item for item in data if "서울" in item.get("addr", "")]
    return filtered_data


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


# 1. 함수 실행 (기존 API 호출 및 DB 업데이트 함수)
def scheduled_task():
    print("스케줄러 실행: 데이터 업데이트 시작")
    result = insert_openapi_wl_obs()  # 기존 함수 호출
    print(f"업데이트 결과: {result}")

# 2. 스케줄러 설정 (백그라운드 실행)
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, "interval", minutes=10)  # 10분마다 실행
scheduler.start()

# 3. FastAPI 종료 시 스케줄러 종료
@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()