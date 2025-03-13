from apscheduler.schedulers.background import BackgroundScheduler
import oracledb, requests
import time

DB_CONFIG = {
    "user": "SAMA",
    "password": "SAMA1234",
    "dsn": "192.168.100.30:1522/XE"
}

# DB 연결 함수
def get_db_connection():
    try:
        return oracledb.connect(**DB_CONFIG)
    except Exception as e:
        print(f"DB 연결 오류: {e}")
        return None


def insert_openapi_wl_obs():
    conn = get_db_connection()
    if conn is None:
        print("Database connection failed")
        return "fail"

    data = get_wl_obs_fromapi()
    print("가져온 데이터:",data)
    if not data:
        print("No data retrieved from API")
        return "fail"
    if conn:
        print("DB 연결 성공")
    else:
        print("DB 연결 실패")

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

    try:
        with conn.cursor() as cursor:
            for item in data:
                try:
                    cursor.execute(insert_sql, (
                        safe_float(item["wlobscd"]), item["obsnm"], item["agcnm"], item["addr"], item["etcaddr"],
                        dms_to_decimal(item["lon"]), dms_to_decimal(item["lat"]), safe_float(item["gdt"]),
                        safe_float(item["attwl"]),
                        safe_float(item["wrnwl"]), safe_float(item["almwl"]), safe_float(item["srswl"]),
                        safe_float(item["pfh"]),
                        item["fstnyn"]
                    ))
                except Exception as e:
                    print(f"삽입 오류: {e} (데이터: {item})")

            conn.commit()
    except Exception as e:
        print(f"DB 작업 중 오류 발생: {e}")
    finally:
        conn.close()

    print("데이터 삽입 완료")
    return "success"


def get_wl_obs_fromapi():
    appid = 'C03BC3D1-443D-46EE-A91F-168CEFDD65A6'
    url = f"https://api.hrfco.go.kr/{appid}/waterlevel/info.json"

    print(f'요청 URL: {url}')

    try:
        response = requests.get(url, timeout=10)  # 타임아웃 설정
        response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
        data = response.json().get('content', [])
        print("API 응답 데이터:", data)

        # "addr" 필드에 "서울"이 포함된 데이터만 필터링
        return [item for item in data if "서울" in item.get("addr", "")]
    except requests.exceptions.RequestException as e:
        print(f"API 요청 오류: {e}")
        return []


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
