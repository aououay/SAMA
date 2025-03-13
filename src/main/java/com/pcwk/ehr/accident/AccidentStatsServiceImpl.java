package com.pcwk.ehr.accident;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AccidentStatsServiceImpl implements AccidentStatsService {
	
	
	@Autowired
    private AccidentStatsMapper accidentStatsMapper;
	
	
	@Override
    public List<AccidentStats> getRecent8WeeksAccidentStats() {
        return accidentStatsMapper.getRecent8WeeksAccidentStats();
    }


	@Override
	public List<AccidentStats> getRecent8MonthsAccidentStats() {		
		return accidentStatsMapper.getRecent8MonthsAccidentStats();
	}
	
	@Override
	public YearlyComparisonDTO getYearlyComparisonStats() {
	    return accidentStatsMapper.getYearlyComparisonStats();
	}


}
