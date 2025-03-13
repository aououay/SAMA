package com.pcwk.ehr.weather_forecast.domain;

import java.util.Objects;

public class Weather_ForecastVO {

	private long fcstId;
	private String guCd;
	private String fcstDt;
	private double temp;
	private double minTemp;
	private double maxTemp;
	private double feelsLikeTemp;
	private String weatherDesc;
	private String weatherIconId;
	private int humidity;
	private double windSpeed;
	private double pop;
	private int crtBy;
	private String crtDt;
	private int modBy;
	private String modDt;
	private String week;

	public Weather_ForecastVO() {
		super();
	}

	public long getFcstId() {
		return fcstId;
	}

	public void setFcstId(long fcstId) {
		this.fcstId = fcstId;
	}

	public String getGuCd() {
		return guCd;
	}

	public void setGuCd(String guCd) {
		this.guCd = guCd;
	}

	public String getFcstDt() {
		return fcstDt;
	}

	public void setFcstDt(String fcstDt) {
		this.fcstDt = fcstDt;
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

	public double getPop() {
		return pop;
	}

	public void setPop(double pop) {
		this.pop = pop;
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

	public String getWeek() {
		return week;
	}

	public void setWeek(String week) {
		this.week = week;
	}

	@Override
	public int hashCode() {
		return Objects.hash(fcstId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Weather_ForecastVO other = (Weather_ForecastVO) obj;
		return fcstId == other.fcstId;
	}

	@Override
	public String toString() {
		return "Weather_ForecastVO [fcstId=" + fcstId + ", guCd=" + guCd + ", fcstDt=" + fcstDt + ", temp=" + temp
				+ ", minTemp=" + minTemp + ", maxTemp=" + maxTemp + ", feelsLikeTemp=" + feelsLikeTemp
				+ ", weatherDesc=" + weatherDesc + ", weatherIconId=" + weatherIconId + ", humidity=" + humidity
				+ ", windSpeed=" + windSpeed + ", pop=" + pop + ", crtBy=" + crtBy + ", crtDt=" + crtDt + ", modBy="
				+ modBy + ", modDt=" + modDt + ", week=" + week + "]";
	}

}
