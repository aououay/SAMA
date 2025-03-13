package com.pcwk.ehr.accident;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface AccidentStatsMapper {
	
	 //최근 8주 데이터 통계 (주간 통계)
     List<AccidentStats> getRecent8WeeksAccidentStats();
    
     // 최근 8개월 데이터 조회 (월간 통계)
     List<AccidentStats> getRecent8MonthsAccidentStats();
     
     //전년 동월 대비 (1달만 비교)
     YearlyComparisonDTO getYearlyComparisonStats();


}
