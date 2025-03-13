package com.pcwk.ehr.air_quality.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.air_quality.domain.Air_QualityVO;
import com.pcwk.ehr.air_quality.domain.Pm_WarningVO;
import com.pcwk.ehr.mapper.Air_QualityMapper;
import com.pcwk.ehr.mapper.Weather_CurrentMapper;
import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;

@Service
public class Air_QualityServiceImpl implements Air_QualityService {

	@Autowired
	Air_QualityMapper air_QualityMapper;

	@Override
	public Air_QualityVO get_Current_Air_QualityBy_Gu_Cd(String gu_Code) {

		Air_QualityVO air_QualityVO = air_QualityMapper.get_Current_Air_QualityBy_Gu_Cd(gu_Code);

		return air_QualityVO;

	}

	@Override
	public List<Pm_WarningVO> get_Pm_Warning_H() {
		
		List<Pm_WarningVO> pm_Warning_List = air_QualityMapper.get_Pm_Warning_H();
		
		return pm_Warning_List;
	}

	@Override
	public List<Air_QualityVO> get_Current_Air_Quality() {

		List<Air_QualityVO> aq_List = air_QualityMapper.get_Current_Air_Quality();
		
		return aq_List;
		
	}

}
