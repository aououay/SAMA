package com.pcwk.ehr.accident;

import java.util.List;

public interface AccidentStatsService {
	
	 List<AccidentStats> getRecent8WeeksAccidentStats();
	 
	 List<AccidentStats> getRecent8MonthsAccidentStats();
	 
	 YearlyComparisonDTO getYearlyComparisonStats();


}
