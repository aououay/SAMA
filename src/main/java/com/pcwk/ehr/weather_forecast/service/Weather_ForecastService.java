package com.pcwk.ehr.weather_forecast.service;

import java.util.List;

import com.pcwk.ehr.weather_forecast.domain.Weather_ForecastVO;

public interface Weather_ForecastService {
	
	/**
	 * 자치구 코드로 5일 날씨 예보 조회
	 * @return Weather_CurrentVO
	 */
	public List<Weather_ForecastVO> get_Weather_ForecastBy_Gu_Cd(String gu_Code);
	
	/**
	 * 자치구 코드로 3일 날씨예보 조회
	 * @return Weather_CurrentVO
	 */
	public List<Weather_ForecastVO> get_Day3_Weather_ForecastBy_Gu_Cd(String gu_Code);

}
