package com.pcwk.ehr.rainfall;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RainfallServiceImpl implements RainfallService {
	
	private final RainfallMapper rainfallMapper;

    @Autowired
    public RainfallServiceImpl(RainfallMapper rainfallMapper) {
        this.rainfallMapper = rainfallMapper;
    }

	@Override
	public List<Rainfall> getAllRainfallStations() {
		return rainfallMapper.getAllRainfallStations();
	}

	@Override
	public List<Rainfall> getLatestRainfallData() {
		 return rainfallMapper.getLatestRainfallData();
	}

	@Override
	public List<Rainfall> getRecentRainfall(int rfObsCd) {
	    List<Rainfall> result = rainfallMapper.getRecentRainfall(rfObsCd);
	    System.out.println("전달할 데이터: " + result);
	    return result;
	}

	@Override
	public List<Rainfall> getGuRainfall() {
	    List<Rainfall> result = rainfallMapper.getGuRainfall();
	    System.out.println("가져온 강수량 데이터: " + result);
	    return result;
	}

	@Override
	public List<Rainfall> getGuList() {
		return rainfallMapper.getGuList();
	}
	
	@Override
	public List<Rainfall> getRainfallWarnings() {
		 List<Rainfall> warnings = rainfallMapper.getRainfallWarnings();
		System.out.println("서비스에서 가져온 특보 데이터: " + warnings);
	    return warnings;
	}


   

	

}
