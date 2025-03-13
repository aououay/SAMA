package com.pcwk.ehr.waterlevel;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;

@Mapper
public interface WaterLevelMapper {
     
	
	List<WaterLevel> getRecentWaterLevels(@Param("wlObsCd") int wlObsCd);
	 
	List<WaterLevel> getAllWaterLevels();
	
	List<WaterLevel> getWaterLevelsState();
	
	List<WaterLevel> getPagedWaterLevels(@Param("offset") int offset, @Param("limit") int limit);
	
	int getTotalRecords();
	
	List<WaterLevel> getLatestWaterLevels();
}