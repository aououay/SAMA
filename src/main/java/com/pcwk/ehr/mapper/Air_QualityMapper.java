package com.pcwk.ehr.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import com.pcwk.ehr.air_quality.domain.Air_QualityVO;
import com.pcwk.ehr.air_quality.domain.Pm_WarningVO;
import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;

@Mapper
public interface Air_QualityMapper {

	/**
	 * 자치구 코드로 현재 대기질 최근 데이터 1건 조회
	 * 
	 * @return Weather_CurrentVO
	 */
	public Air_QualityVO get_Current_Air_QualityBy_Gu_Cd(String gu_Code);
	
	public List<Pm_WarningVO> get_Pm_Warning_H();
	
	public List<Air_QualityVO> get_Current_Air_Quality();

}
