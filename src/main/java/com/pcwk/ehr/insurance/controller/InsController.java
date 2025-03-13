package com.pcwk.ehr.insurance.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.pcwk.ehr.insurance.domain.InsVO;
import com.pcwk.ehr.insurance.service.InsService;

@RequestMapping("ins")
@Controller
public class InsController {

	final Logger log = LoggerFactory.getLogger(getClass());
	
	@Autowired
	InsService insService;

	public InsController() {
		log.info("┌──────────────────────────┐");
		log.info("│ InsController()          │");
		log.info("└──────────────────────────┘");
	}
	
	@GetMapping("/getIns.do")
	public String getIns(@RequestParam(name="insSggNm", required = false, defaultValue = "None") String insSggNm
						,@RequestParam(name="insStartDt", required = false, defaultValue = "20250101") String insStartDt
						,Model model) {
		log.info("┌──────────────────────────┐");
		log.info("│ getIns()                 │");
		log.info("└──────────────────────────┘");
		
		String time = insService.getTime();
		log.info("time:{}",time);
		
		 // InsVO 객체에 값 설정
        InsVO insVO = new InsVO();
        insVO.setInsSggNm(insSggNm);
        insVO.setInsStartDt(insStartDt);
	
        log.info("insSggNm:{}",insSggNm);
        log.info("insStartDt:{}",insStartDt);
        
		List<InsVO> ins = insService.getIns(insVO);
		log.info("ins:{}",ins);
		
		List<InsVO> grnt = insService.getGrnt(insVO);
		log.info("ins:{}",grnt);
		
		List<InsVO> sggNames = insService.sggNames(insVO);
		
		model.addAttribute("time", time);
		model.addAttribute("ins", ins);
		model.addAttribute("grnt", grnt);
		model.addAttribute("sggNames", sggNames);
		
		return "ins/ins";
	}
	
	@GetMapping("/intro.do")
	public String intro() {
		log.info("┌──────────────────────────┐");
		log.info("│ intro()                  │");
		log.info("└──────────────────────────┘");
		
		return "ins/intro";
	}
	
	@GetMapping("/main")
	public String main() {
		log.info("┌──────────────────────────┐");
		log.info("│ main()                   │");
		log.info("└──────────────────────────┘");
		
		return "template/main";
	}
	
}
