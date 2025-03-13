package com.pcwk.ehr.rainfall;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;


@Mapper
public interface RainfallMapper {
	
    List<Rainfall> getAllRainfallStations();
    
    List<Rainfall> getLatestRainfallData();
    
    List<Rainfall> getRainfallByStation(@Param("rfObsCd") int rfObsCd);
    
    List<Rainfall> getRecentRainfall(int rfObsCd);
    
    List<Rainfall> getGuRainfall();
    
    List<Rainfall> getGuList(); // 자치구 목록 
    
    List<Rainfall> getRainfallWarnings();

	
}


