package com.pcwk.ehr.accident;



import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class AccidentStatsController {

    @Autowired
    private AccidentStatsService accidentStatsService;

    @GetMapping("/accident-stats")
    public String getAccidentStats(Model model) {
    	model.addAttribute("accidentStats", accidentStatsService.getRecent8WeeksAccidentStats());
        return "accident/accidentStats"; // JSP 뷰 이름
    }
    
    @GetMapping("/accident-stats/data")
    @ResponseBody
    public List<AccidentStats> getAccidentStatsData() {
        return accidentStatsService.getRecent8WeeksAccidentStats();
    }
    
    @GetMapping("/accident-stats/monthly-data")
    @ResponseBody
    public List<AccidentStats> getMonthlyAccidentStatsData() {
        return accidentStatsService.getRecent8MonthsAccidentStats();
    }
    
    @GetMapping("/accident-stats/yearly-comparison")
    @ResponseBody
    public YearlyComparisonDTO getYearlyComparisonStats() {
        return accidentStatsService.getYearlyComparisonStats();
    }

}

