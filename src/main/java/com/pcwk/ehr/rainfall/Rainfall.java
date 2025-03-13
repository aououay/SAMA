package com.pcwk.ehr.rainfall;

public class Rainfall {
    
    private String guCd;  // 자치구 코드
    private String guNm;  // 자치구 이름
    private int rfObsCd;  // 강수량 관측소 코드
    private String meaDt;  //강수량 측정시간
    private double rf;  // 강수량(mm)
    private double rf_1h;  // 1시간 강우량
    private double rf_24h; // 24시간 강우량
    private double rf_48h; // 48시간 강우량
    private String regDt; //  등록일
    private String obsNm;  // 관측소 이름
    private String addr;  // 관측소 주소
    private String etcAddr;  // 관측소 상세주소
    private double lon;  // 관측소 경도
    private double lat;  // 관측소 위도
    private double msLat;  // 자치구 위도
    private double msLon;  // 자치구 경도
    private String hourState; //시간당 상
    private String state; //예보종류
    private String forecastState; //예보상태
    private String forecastNumber; //예보번호
    
	public Rainfall() {
		super();
	}

	
	public String getGuCd() {
		return guCd;
	}


	public void setGuCd(String guCd) {
		this.guCd = guCd;
	}


	public String getGuNm() {
		return guNm;
	}

	public void setGuNm(String guNm) {
		this.guNm = guNm;
	}

	public int getRfObsCd() {
		return rfObsCd;
	}

	public void setRfObsCd(int rfObsCd) {
		this.rfObsCd = rfObsCd;
	}

	public String getMeaDt() {
		return meaDt;
	}

	public void setMeaDt(String meaDt) {
		this.meaDt = meaDt;
	}

	public double getRf() {
		return rf;
	}

	public void setRf(double rf) {
		this.rf = rf;
	}

	public double getRf_1h() {
		return rf_1h;
	}


	public void setRf_1h(double rf_1h) {
		this.rf_1h = rf_1h;
	}


	public double getRf_24h() {
		return rf_24h;
	}


	public void setRf_24h(double rf_24h) {
		this.rf_24h = rf_24h;
	}


	public double getRf_48h() {
		return rf_48h;
	}


	public void setRf_48h(double rf_48h) {
		this.rf_48h = rf_48h;
	}


	public String getRegDt() {
		return regDt;
	}

	public void setRegDt(String regDt) {
		this.regDt = regDt;
	}

	public String getObsNm() {
		return obsNm;
	}

	public void setObsNm(String obsNm) {
		this.obsNm = obsNm;
	}

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public String getEtcAddr() {
		return etcAddr;
	}

	public void setEtcAddr(String etcAddr) {
		this.etcAddr = etcAddr;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getMsLat() {
		return msLat;
	}

	public void setMsLat(double msLat) {
		this.msLat = msLat;
	}

	public double getMsLon() {
		return msLon;
	}

	public void setMsLon(double msLon) {
		this.msLon = msLon;
	}
	

	public String getHourState() {
		return hourState;
	}


	public void setHourState(String hourState) {
		this.hourState = hourState;
	}


	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getForecastState() {
		return forecastState;
	}

	public void setForecastState(String forecastState) {
		this.forecastState = forecastState;
	}

	public String getForecastNumber() {
		return forecastNumber;
	}

	public void setForecastNumber(String forecastNumber) {
		this.forecastNumber = forecastNumber;
	}


	@Override
	public String toString() {
		return "Rainfall [guCd=" + guCd + ", guNm=" + guNm + ", rfObsCd=" + rfObsCd + ", meaDt=" + meaDt + ", rf=" + rf
				+ ", rf_1h=" + rf_1h + ", rf_24h=" + rf_24h + ", rf_48h=" + rf_48h + ", regDt=" + regDt + ", obsNm="
				+ obsNm + ", addr=" + addr + ", etcAddr=" + etcAddr + ", lon=" + lon + ", lat=" + lat + ", msLat="
				+ msLat + ", msLon=" + msLon + ", hourState=" + hourState + ", state=" + state + ", forecastState="
				+ forecastState + ", forecastNumber=" + forecastNumber + ", toString()=" + super.toString() + "]";
	}

    
}
