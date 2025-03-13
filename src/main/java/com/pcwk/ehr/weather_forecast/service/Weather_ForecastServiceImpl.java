package com.pcwk.ehr.weather_forecast.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.cmn.DateCmn;
import com.pcwk.ehr.mapper.Weather_CurrentMapper;
import com.pcwk.ehr.mapper.Weather_ForecastMapper;
import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;
import com.pcwk.ehr.weather_forecast.domain.Weather_ForecastVO;

@Service
public class Weather_ForecastServiceImpl implements Weather_ForecastService {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	Weather_ForecastMapper weather_ForecastMapper;

	@Override
	public List<Weather_ForecastVO> get_Weather_ForecastBy_Gu_Cd(String gu_Code) {

		String now = DateCmn.now_Str("23:59");

		List<Weather_ForecastVO> wf_List = weather_ForecastMapper.get_Weather_ForecastBy_Gu_Cd(gu_Code, now);

		return wf_List;

	}

	@Override
	public List<Weather_ForecastVO> get_Day3_Weather_ForecastBy_Gu_Cd(String gu_Code) {
		
		String now = DateCmn.now_Str("00:00");

		List<Weather_ForecastVO> wf3_List = weather_ForecastMapper.get_Day3_Weather_ForecastBy_Gu_Cd(gu_Code, now);

		return wf3_List;
	}

}
