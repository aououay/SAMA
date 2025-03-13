package com.pcwk.ehr.weather_current.service;

import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;

public interface Weather_CurrentService {
	
	/**
	 * 자치구 코드로 현재 날씨 조회
	 * @return Weather_CurrentVO
	 */
	public Weather_CurrentVO get_Current_WeatherBy_Gu_Cd(String gu_Code);

}
