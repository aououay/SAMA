package com.pcwk.ehr.weather_current.domain;

import java.util.Objects;

public class Weather_CurrentVO {

	private long weatherId;
	private String guCd;
	private double temp;
	private double minTemp;
	private double maxTemp;
	private double feelsLikeTemp;
	private String weatherDesc;
	private String weatherIconId;
	private int humidity;
	private double windSpeed;
	private String msDt;
	private int crtBy;
	private String crtDt;
	private int modBy;
	private String modDt;
	private String guNm;

	public Weather_CurrentVO() {
		super();

	}

	public long getWeatherId() {
		return weatherId;
	}

	public void setWeatherId(long weatherId) {
		this.weatherId = weatherId;
	}

	public String getGuCd() {
		return guCd;
	}

	public void setGuCd(String guCd) {
		this.guCd = guCd;
	}

	public double getTemp() {
		return temp;
	}

	public void setTemp(double temp) {
		this.temp = temp;
	}

	public double getMinTemp() {
		return minTemp;
	}

	public void setMinTemp(double minTemp) {
		this.minTemp = minTemp;
	}

	public double getMaxTemp() {
		return maxTemp;
	}

	public void setMaxTemp(double maxTemp) {
		this.maxTemp = maxTemp;
	}

	public double getFeelsLikeTemp() {
		return feelsLikeTemp;
	}

	public void setFeelsLikeTemp(double feelsLikeTemp) {
		this.feelsLikeTemp = feelsLikeTemp;
	}

	public String getWeatherDesc() {
		return weatherDesc;
	}

	public void setWeatherDesc(String weatherDesc) {
		this.weatherDesc = weatherDesc;
	}

	public String getWeatherIconId() {
		return weatherIconId;
	}

	public void setWeatherIconId(String weatherIconId) {
		this.weatherIconId = weatherIconId;
	}

	public int getHumidity() {
		return humidity;
	}

	public void setHumidity(int humidity) {
		this.humidity = humidity;
	}

	public double getWindSpeed() {
		return windSpeed;
	}

	public void setWindSpeed(double windSpeed) {
		this.windSpeed = windSpeed;
	}

	public String getMsDt() {
		return msDt;
	}

	public void setMsDt(String msDt) {
		this.msDt = msDt;
	}

	public int getCrtBy() {
		return crtBy;
	}

	public void setCrtBy(int crtBy) {
		this.crtBy = crtBy;
	}

	public String getCrtDt() {
		return crtDt;
	}

	public void setCrtDt(String crtDt) {
		this.crtDt = crtDt;
	}

	public int getModBy() {
		return modBy;
	}

	public void setModBy(int modBy) {
		this.modBy = modBy;
	}

	public String getModDt() {
		return modDt;
	}

	public void setModDt(String modDt) {
		this.modDt = modDt;
	}

	public String getGuNm() {
		return guNm;
	}

	public void setGuNm(String guNm) {
		this.guNm = guNm;
	}

	@Override
	public int hashCode() {
		return Objects.hash(weatherId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Weather_CurrentVO other = (Weather_CurrentVO) obj;
		return weatherId == other.weatherId;
	}

	@Override
	public String toString() {
		return "Weather_CurrentVO [weatherId=" + weatherId + ", guCd=" + guCd + ", temp=" + temp + ", minTemp="
				+ minTemp + ", maxTemp=" + maxTemp + ", feelsLikeTemp=" + feelsLikeTemp + ", weatherDesc=" + weatherDesc
				+ ", weatherIconId=" + weatherIconId + ", humidity=" + humidity + ", windSpeed=" + windSpeed + ", msDt="
				+ msDt + ", crtBy=" + crtBy + ", crtDt=" + crtDt + ", modBy=" + modBy + ", modDt=" + modDt + ", guNm="
				+ guNm + "]";
	}
	
	
	
	
}
