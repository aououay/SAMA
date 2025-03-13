package com.pcwk.ehr.waterlevel;

import java.util.List;

public interface WaterLevelService {
	
	List<WaterLevel> getAllWaterLevels();
	
	List<WaterLevel> getRecentWaterLevels(int wlObsCd);
	
	List<WaterLevel> getWaterLevelsState();
	
	List<WaterLevel> getPagedWaterLevels(int offset, int limit);
	
	int getTotalRecords();
	
	List<WaterLevel> getLatestWaterLevels();
}
