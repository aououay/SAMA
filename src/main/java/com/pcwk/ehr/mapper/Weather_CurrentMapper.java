package com.pcwk.ehr.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;

@Mapper
public interface Weather_CurrentMapper {

	/**
	 * 자치구 코드로 현재 날씨 최근 데이터 1건 조회
	 * 
	 * @return Weather_CurrentVO
	 */
	public Weather_CurrentVO get_Current_WeatherBy_Gu_Cd(String gu_Code);

	@Select("SELECT SYSDATE FROM dual")
	public String getDateTime();

}
