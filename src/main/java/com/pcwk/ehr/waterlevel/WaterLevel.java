package com.pcwk.ehr.waterlevel;

public class WaterLevel {
    private int wlObsCd; //관측소 코드
    private String obsNm; //관측소 이름
    private String addr; //주소
    private String etcAddr; //상세주소
    private double attWl; //관심 수위
    private double wrnWl; //주의보 수위
    private double almWl; //경보 수위
    private double fw; //유량자료
    private double wl; //수위자료
    private double lat;  //위도
    private double lon; //경도
    private String meaDt; //측정 시간
    private String regDt; //등록 시간
    private String state; //예보종류
    private String forecastState; //예보상태
    private String forecastNumber; //예보번호
    
	public WaterLevel() {
		super();
	}

	public int getWlObsCd() {
		return wlObsCd;
	}

	public void setWlObsCd(int wlObsCd) {
		this.wlObsCd = wlObsCd;
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

	public double getAttWl() {
		return attWl;
	}

	public void setAttWl(double attWl) {
		this.attWl = attWl;
	}

	public double getWrnWl() {
		return wrnWl;
	}

	public void setWrnWl(double wrnWl) {
		this.wrnWl = wrnWl;
	}

	public double getAlmWl() {
		return almWl;
	}

	public void setAlmWl(double almWl) {
		this.almWl = almWl;
	}

	public double getFw() {
		return fw;
	}

	public void setFw(double fw) {
		this.fw = fw;
	}

	public double getWl() {
		return wl;
	}

	public void setWl(double wl) {
		this.wl = wl;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public String getMeaDt() {
		return meaDt;
	}

	public void setMeaDt(String meaDt) {
		this.meaDt = meaDt;
	}

	public String getRegDt() {
		return regDt;
	}

	public void setRegDt(String regDt) {
		this.regDt = regDt;
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

}
