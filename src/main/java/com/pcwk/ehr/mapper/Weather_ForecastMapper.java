package com.pcwk.ehr.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;
import com.pcwk.ehr.weather_forecast.domain.Weather_ForecastVO;

@Mapper
public interface Weather_ForecastMapper {

	/**
	 * 자치구 코드로 5일간 날씨예보 데이터 조회
	 * 
	 * @return Weather_CurrentVO
	 */
	List<Weather_ForecastVO> get_Weather_ForecastBy_Gu_Cd(@Param("gu_Code") String gu_Code, @Param("now")String now);
	
	/**
	 * 자치구 코드로 3일 날씨예보 조회
	 * @return Weather_CurrentVO
	 */
	List<Weather_ForecastVO> get_Day3_Weather_ForecastBy_Gu_Cd(@Param("gu_Code") String gu_Code, @Param("now")String now);



}
