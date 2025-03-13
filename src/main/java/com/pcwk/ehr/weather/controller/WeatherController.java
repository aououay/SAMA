package com.pcwk.ehr.weather.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import com.pcwk.ehr.air_quality.domain.Air_QualityVO;
import com.pcwk.ehr.air_quality.service.Air_QualityService;
import com.pcwk.ehr.cmn.DateCmn;
import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;
import com.pcwk.ehr.weather_current.service.Weather_CurrentService;
import com.pcwk.ehr.weather_forecast.domain.Weather_ForecastVO;
import com.pcwk.ehr.weather_forecast.service.Weather_ForecastService;

@Controller
public class WeatherController {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	private Weather_CurrentService weather_CurrentService;

	@Autowired
	private Air_QualityService air_QualityService;

	@Autowired
	private Weather_ForecastService weather_ForecastService;

	public WeatherController() {
		super();
		log.info("┌──────────────────────┐");
		log.info("│  WeatherController() │");
		log.info("└──────────────────────┘");
	}

	@GetMapping({ "weather/{gu_Code}", "weather" })
	public String weatherView(@PathVariable(value = "gu_Code", required = false) String gu_Code, Model model) {

		log.info("┌──────────────────────┐");
		log.info("│  WeatherController() │");
		log.info("│     weatherView()    │");
		log.info("└──────────────────────┘");

		if (gu_Code == null) {
			gu_Code = "111261";
		}

		Weather_CurrentVO weather_CurrentVO = weather_CurrentService.get_Current_WeatherBy_Gu_Cd(gu_Code);
		Air_QualityVO air_QualityVO = air_QualityService.get_Current_Air_QualityBy_Gu_Cd(gu_Code);
		List<Weather_ForecastVO> wf_List = weather_ForecastService.get_Weather_ForecastBy_Gu_Cd(gu_Code);

		List<Weather_ForecastVO> wf3_List = weather_ForecastService.get_Day3_Weather_ForecastBy_Gu_Cd(gu_Code);

		log.info("현재 날씨 : " + weather_CurrentVO.toString());
		log.info("현재 대기질 : " + air_QualityVO.toString());

		int count = 1;

		for (Weather_ForecastVO wf : wf_List) {

			wf.setWeek(DateCmn.str_To_Week(wf.getFcstDt()));

			wf.setFcstDt(wf.getFcstDt().substring(5, 10).replace("-", "/"));

			log.info(count + ":" + wf.toString());

		}

		for (Weather_ForecastVO wf : wf3_List) {
			log.info("3일 예보 :" + wf.toString());
		}

		model.addAttribute("wc", weather_CurrentVO);
		model.addAttribute("aqc", air_QualityVO);
		model.addAttribute("wf_List", wf_List);

		model.addAttribute("wf3_List", wf3_List);

		model.addAttribute("wc_Dt", weather_CurrentVO.getMsDt().split(" ")[1]);
		model.addAttribute("aqc_Dt", air_QualityVO.getMsDt().split(" ")[1]);

		return "weather/weather";
	}

}
