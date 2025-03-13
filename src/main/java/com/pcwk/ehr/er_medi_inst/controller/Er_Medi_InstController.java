package com.pcwk.ehr.er_medi_inst.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import com.pcwk.ehr.air_quality.domain.Pm_WarningVO;
import com.pcwk.ehr.air_quality.service.Air_QualityService;
import com.pcwk.ehr.cmn.Er_Medi_Inst_Cmn;
import com.pcwk.ehr.er_medi_inst.domain.Er_BedVO;
import com.pcwk.ehr.er_medi_inst.domain.Er_Medi_InstVO;
import com.pcwk.ehr.er_medi_inst.domain.Er_Medi_Inst_Er_BedVO;
import com.pcwk.ehr.er_medi_inst.service.Er_Medi_InstService;

@Controller
public class Er_Medi_InstController {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	private Er_Medi_InstService er_Medi_InstService;

	public Er_Medi_InstController() {
		super();
		log.info("┌────────────────────────────┐");
		log.info("│  Er_Medi_InstController()  │");
		log.info("└────────────────────────────┘");
	}

	@GetMapping("er_medi_inst/list")
	public String er_Medi_Inst_ListView(@RequestParam(value = "gu_Code", defaultValue = "") String gu_Code,
			@RequestParam(value = "duty_Nm", defaultValue = "") String duty_Nm, Model model) {

		log.info("┌────────────────────────────┐");
		log.info("│  Er_Medi_InstController()  │");
		log.info("│   er_Medi_Inst_ListView()  │");
		log.info("└────────────────────────────┘");

		log.info("기관명 : " + duty_Nm);
		log.info("구코드 : " + gu_Code);

		List<Er_Medi_InstVO> er_Medi_Inst_List = er_Medi_InstService.get_Er_Medi_Inst_Info(gu_Code, duty_Nm);

		model.addAttribute("er_Medi_Inst_List", er_Medi_Inst_List);

		model.addAttribute("gu_Nm", Er_Medi_Inst_Cmn.gu_Nm_From_Gu_Code(gu_Code));
		model.addAttribute("duty_Nm", duty_Nm);

		return "er_medi_inst/er_medi_inst_list";
	}

	@GetMapping("er_medi_inst/detail/{hpid}")
	public String er_Medi_Inst_DetailView(@PathVariable(value = "hpid") String hpid, Model model) {

		log.info("┌────────────────────────────┐");
		log.info("│  Er_Medi_InstController()  │");
		log.info("│  er_Medi_Inst_DetailView() │");
		log.info("└────────────────────────────┘");

		log.info("기관코드 : " + hpid);

		Er_Medi_Inst_Er_BedVO emi_Info = er_Medi_InstService.get_Er_Medi_Inst_And_Er_Bed_Info(hpid);

		if (emi_Info == null) {
			return "redirect:/er_medi_inst/list";
		}

		model.addAttribute("emi_Info", emi_Info);

		return "er_medi_inst/er_medi_inst_detail";
	}

	@GetMapping("er_medi_inst/er_bed")
	public String er_BedView(@RequestParam(value = "gu_Code", defaultValue = "") String gu_Code,
			@RequestParam(value = "duty_Nm", defaultValue = "") String duty_Nm, Model model) {

		log.info("┌────────────────────────────┐");
		log.info("│  Er_Medi_InstController()  │");
		log.info("│        er_BedView()        │");
		log.info("└────────────────────────────┘");

		log.info("기관명 : " + duty_Nm);
		log.info("구코드 : " + gu_Code);

		List<Er_BedVO> er_Bed_List = er_Medi_InstService.get_Current_Er_Bed(gu_Code, duty_Nm);

		model.addAttribute("er_Bed_List", er_Bed_List);
		model.addAttribute("gu_Nm", Er_Medi_Inst_Cmn.gu_Nm_From_Gu_Code(gu_Code));
		model.addAttribute("duty_Nm", duty_Nm);

		return "er_medi_inst/er_bed";
	}

}
