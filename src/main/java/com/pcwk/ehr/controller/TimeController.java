package com.pcwk.ehr.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.pcwk.ehr.service.TimeService;

@Controller
@RequestMapping("time")
public class TimeController {


	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	@Qualifier("timeServiceImpl")
	TimeService timeService;

	public TimeController() {
		super();
		log.info("┌─────────────────────┐");
		log.info("│  TimeServiceImpl()  │");
		log.info("└─────────────────────┘");
	}

	@GetMapping("/getNewDateTime")
	public String getNewDateTime(Model model) {

		log.info("┌────────────────────┐");
		log.info("│  getNewDateTime()  │");
		log.info("└────────────────────┘");

		String dateTime = timeService.getNewDateTime();
		log.info("dateTime : {}", dateTime);

		model.addAttribute("dateTime", dateTime);

		return "time/time";


	}
	
	@GetMapping("/home")
	public String home(Model model) {
		
		log.info("┌────────────────────┐");
		log.info("│  getNewDateTime()  │");
		log.info("└────────────────────┘");
		
		
		return "index";

		
	}
	
	
}
