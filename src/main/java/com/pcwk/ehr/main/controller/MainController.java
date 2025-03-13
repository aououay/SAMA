package com.pcwk.ehr.main.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import com.pcwk.ehr.accident.AccidentStatsService;
import com.pcwk.ehr.air_quality.domain.Air_QualityVO;
import com.pcwk.ehr.air_quality.service.Air_QualityService;
import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.board.service.BoardService;
import com.pcwk.ehr.cmn.DateCmn;
import com.pcwk.ehr.disastermessage.domain.DisasterMessageVO;
import com.pcwk.ehr.disastermessage.service.DisasterMessageService;
import com.pcwk.ehr.videopost.domain.VideoPostVO;
import com.pcwk.ehr.videopost.service.VideoPostService;
import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;
import com.pcwk.ehr.weather_current.service.Weather_CurrentService;
import com.pcwk.ehr.weather_forecast.domain.Weather_ForecastVO;
import com.pcwk.ehr.weather_forecast.service.Weather_ForecastService;

@Controller
public class MainController {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	private Weather_CurrentService weather_CurrentService;

	@Autowired
	private Air_QualityService air_QualityService;

	@Autowired
	private Weather_ForecastService weather_ForecastService;

	@Autowired
	private VideoPostService videoPostService;

	@Autowired
	private DisasterMessageService disasterMessageService;
	
	@Autowired
	private BoardService boardService;
	

	public MainController() {
		super();
		log.info("┌────────────────────┐");
		log.info("│  MainController()  │");
		log.info("└────────────────────┘");
	}

	@GetMapping({ "index/{gu_Code}", "index" })
	public String indexView(@PathVariable(value = "gu_Code", required = false) String gu_Code, Model model) {

		log.info("┌────────────────────┐");
		log.info("│  MainController()  │");
		log.info("│    indexView()     │");
		log.info("└────────────────────┘");

		String videoUUID = "891705d5-f330-4fee-91c8-83476abb05f9";

		if (gu_Code == null) {
			gu_Code = "111261";
		}

		Weather_CurrentVO weather_CurrentVO = weather_CurrentService.get_Current_WeatherBy_Gu_Cd(gu_Code);
		Air_QualityVO air_QualityVO = air_QualityService.get_Current_Air_QualityBy_Gu_Cd(gu_Code);
		List<Weather_ForecastVO> wf_List = weather_ForecastService.get_Weather_ForecastBy_Gu_Cd(gu_Code);
		VideoPostVO video = videoPostService.getVideoPostById(videoUUID);
		List<DisasterMessageVO> dmListForseoul = disasterMessageService.getLatestFiveDisasterMessageForSeoul();
		List<BoardVO> noticeList = boardService.getLatestFiveBoard("10");
		List<BoardVO> statsPostList = boardService.getLatestFiveBoard("30");
		

		log.info("현재 날씨 : " + weather_CurrentVO.toString());
		log.info("현재 대기질 : " + air_QualityVO.toString());

		int count = 1;

		for (Weather_ForecastVO wf : wf_List) {

			wf.setWeek(DateCmn.str_To_Week(wf.getFcstDt()));

			wf.setFcstDt(wf.getFcstDt().substring(5, 10).replace("-", "/"));

			log.info(count + ":" + wf.toString());

		}

		for (BoardVO vo : noticeList) {
			vo.setRegDt(vo.getRegDt().substring(0, 10));
		}
		
		for (BoardVO vo : statsPostList) {
			vo.setRegDt(vo.getRegDt().substring(0, 10));
			log.info(vo.toString());
		}

		model.addAttribute("wc", weather_CurrentVO);
		model.addAttribute("aqc", air_QualityVO);
		model.addAttribute("wf_List", wf_List);
		model.addAttribute("video", video);
		model.addAttribute("dmListForseoul", dmListForseoul);
		model.addAttribute("noticeList", noticeList);
		model.addAttribute("statsPostList", statsPostList);

		model.addAttribute("wc_Dt", weather_CurrentVO.getMsDt().split(" ")[1]);
		model.addAttribute("aqc_Dt", air_QualityVO.getMsDt().split(" ")[1]);

		return "index";
	}

}
