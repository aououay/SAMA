package com.pcwk.ehr.waterlevel;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class WaterLevelServiceImpl implements WaterLevelService {
	
	
	final Logger log = LoggerFactory.getLogger(getClass());
	
	private final WaterLevelMapper waterLevelMapper;
	   
    @Autowired
    public WaterLevelServiceImpl(WaterLevelMapper waterLevelMapper) {
        this.waterLevelMapper = waterLevelMapper;
    }
    @Override
    public List<WaterLevel> getAllWaterLevels() {
        return waterLevelMapper.getAllWaterLevels();
    }
    @Override
    public List<WaterLevel> getRecentWaterLevels(int wlObsCd) {
        return waterLevelMapper.getRecentWaterLevels(wlObsCd);
    }
    @Override	
    public List<WaterLevel> getWaterLevelsState() {
    	return waterLevelMapper.getWaterLevelsState();
    }
    @Override
    public List<WaterLevel> getPagedWaterLevels(int offset, int limit) {
    	
    	List<WaterLevel> list =  waterLevelMapper.getPagedWaterLevels(offset, limit);
    	
    	for(WaterLevel wl: list) {
    		
    		log.info("홍수 ID : " + wl.getMeaDt());
    		
    	}
    	
    	
        return list;
    }
    @Override
    public int getTotalRecords() {
        return waterLevelMapper.getTotalRecords();
    }
    @Override
    public List<WaterLevel> getLatestWaterLevels() {
        return waterLevelMapper.getLatestWaterLevels();
    }
}



