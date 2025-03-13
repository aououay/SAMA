# 1. 함수 실행 (기존 API 호출 및 DB 업데이트 함수)
def scheduled_task():
    print("스케줄러 실행: 데이터 업데이트 시작")
    result = insert_rf_obs_gu_info()  # 기존 함수 호출
    print(f"업데이트 결과: {result}")

# 2. 스케줄러 설정 (백그라운드 실행)
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, "interval", minutes=10)  # 10분마다 실행
scheduler.start()


# 즉시 실행
scheduled_task()

# 프로그램이 종료되지 않도록 유지
while True:
    time.sleep(1)