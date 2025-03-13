package com.pcwk.ehr.waterlevel;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/waterlevel")
public class WaterLevelController {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	private final WaterLevelService waterLevelService;

	public WaterLevelController(WaterLevelService waterLevelService) {
		this.waterLevelService = waterLevelService;
	}

	@GetMapping("/view/all")
	public String showWaterLevelPage(Model model) {
		List<WaterLevel> waterLevels = waterLevelService.getWaterLevelsState();

		model.addAttribute("waterLevels", waterLevels);
		return "waterlevel/waterlevel";
	}

	@GetMapping("/recent/{wlObsCd}")
	@ResponseBody
	public List<WaterLevel> getRecentWaterLevels(@PathVariable("wlObsCd") int wlObsCd) {
		return waterLevelService.getRecentWaterLevels(wlObsCd);
	}

	@GetMapping("/view")
    public String showPagedWaterLevelPage(@RequestParam(name = "page", defaultValue = "1") int page, Model model) {
        int pageSize = 10;
        int totalRecords = waterLevelService.getTotalRecords();
        int totalPages = (totalRecords + pageSize - 1) / pageSize;  // 전체 페이지 계산

        // 현재 페이지 범위 조정
        int startPage = Math.max(1, page - 4);
        int endPage = Math.min(startPage + 9, totalPages);

        List<WaterLevel> allWaterLevels = waterLevelService.getAllWaterLevels();
        List<WaterLevel> pagedWaterLevels = waterLevelService.getPagedWaterLevels((page - 1) * pageSize, pageSize);
        List<WaterLevel> latestWaterLevels = waterLevelService.getLatestWaterLevels();

        model.addAttribute("allWaterLevels", allWaterLevels);  
        model.addAttribute("waterLevels", pagedWaterLevels);  
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("startPage", startPage);
        model.addAttribute("endPage", endPage);
        model.addAttribute("showPrev", page > 1);
        model.addAttribute("showNext", page < totalPages);
        model.addAttribute("latestWaterLevels", latestWaterLevels);
        
        for(WaterLevel wl: pagedWaterLevels) {
        	log.info("홍수 테스트" + wl.toString());
        }
        
        

        return "waterlevel/waterlevel";
    
    }

	@GetMapping("/latest")
	@ResponseBody
	public List<WaterLevel> getLatestWaterLevels() {
		return waterLevelService.getLatestWaterLevels();
	}

}
