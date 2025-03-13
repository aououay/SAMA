package com.pcwk.ehr.weather_current.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.mapper.Weather_CurrentMapper;
import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;

@Service
public class Weather_CurrentServiceImpl implements Weather_CurrentService {
	
	@Autowired
	Weather_CurrentMapper weather_CurrentMapper;

	@Override
	public Weather_CurrentVO get_Current_WeatherBy_Gu_Cd(String gu_Code) {
		
		Weather_CurrentVO weather_CurrentVO = weather_CurrentMapper.get_Current_WeatherBy_Gu_Cd(gu_Code);
		
		return weather_CurrentVO;
	}
	
	

}
