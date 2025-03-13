package com.pcwk.ehr.air_quality.service;

import java.util.List;

import com.pcwk.ehr.air_quality.domain.Air_QualityVO;
import com.pcwk.ehr.air_quality.domain.Pm_WarningVO;
import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;

public interface Air_QualityService {
	
	/**
	 * 자치구 코드로 현재 대기질 조회
	 * @return Weather_CurrentVO
	 */
	public Air_QualityVO get_Current_Air_QualityBy_Gu_Cd(String gu_Code);
	
	/**
	 * 미세먼지 경보 발령 내역 조회
	 * @return
	 */
	public List<Pm_WarningVO> get_Pm_Warning_H();
	
	/**
	 * 모든 자치구의 현재 대기질 조회
	 * @return
	 */
	public List<Air_QualityVO> get_Current_Air_Quality();

}
