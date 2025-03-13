package com.pcwk.ehr.rainfall;
import java.util.List;



public interface RainfallService {
	
    List<Rainfall> getAllRainfallStations();
    List<Rainfall> getLatestRainfallData();
    List<Rainfall> getRecentRainfall(int rfObsCd);
    List<Rainfall> getGuRainfall();
    List<Rainfall> getGuList(); // 자치구 목록 
    List<Rainfall> getRainfallWarnings();

	
    
}
