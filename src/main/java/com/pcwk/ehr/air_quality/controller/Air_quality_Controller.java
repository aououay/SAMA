package com.pcwk.ehr.air_quality.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import com.pcwk.ehr.air_quality.domain.Air_QualityVO;
import com.pcwk.ehr.air_quality.domain.Pm_WarningVO;
import com.pcwk.ehr.air_quality.service.Air_QualityService;

@Controller
public class Air_quality_Controller {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	private Air_QualityService air_QualityService;

	public Air_quality_Controller() {
		super();
		log.info("┌────────────────────────────┐");
		log.info("│  Air_quality_Controller()  │");
		log.info("└────────────────────────────┘");
	}

	@GetMapping("air_quality")
	public String air_QualityView(Model model) {

		log.info("┌────────────────────────────┐");
		log.info("│  Air_quality_Controller()  │");
		log.info("│     air_QualityView()      │");
		log.info("└────────────────────────────┘");
		
		
		List<Air_QualityVO> aq_List = air_QualityService.get_Current_Air_Quality();
		
		List<Pm_WarningVO> pm_Warning_List = air_QualityService.get_Pm_Warning_H();
		
		for (Air_QualityVO aq : aq_List) {
			
			log.info("aq : " + aq.toString());
			
		}
		
		model.addAttribute("aq_List", aq_List);
		model.addAttribute("pm_Warning_List", pm_Warning_List);

		return "air_quality/air_quality";
	}

	@GetMapping("air_quality/pm_warning")
	public String pm_WarningView(Model model) {

		log.info("┌────────────────────────────┐");
		log.info("│  Air_quality_Controller()  │");
		log.info("│      pm_WarningView()      │");
		log.info("└────────────────────────────┘");

		List<Pm_WarningVO> pm_Warning_List = air_QualityService.get_Pm_Warning_H();

		model.addAttribute("pm_Warning_List", pm_Warning_List);

		return "air_quality/pm_warning";
	}

}
