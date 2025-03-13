package com.pcwk.ehr.er_medi_inst.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.air_quality.domain.Air_QualityVO;
import com.pcwk.ehr.air_quality.domain.Pm_WarningVO;
import com.pcwk.ehr.er_medi_inst.domain.Er_BedVO;
import com.pcwk.ehr.er_medi_inst.domain.Er_Medi_InstVO;
import com.pcwk.ehr.er_medi_inst.domain.Er_Medi_Inst_Er_BedVO;
import com.pcwk.ehr.mapper.Air_QualityMapper;
import com.pcwk.ehr.mapper.Er_Medi_InstMapper;
import com.pcwk.ehr.mapper.Weather_CurrentMapper;
import com.pcwk.ehr.weather_current.domain.Weather_CurrentVO;

@Service
public class Er_Medi_InstServiceImpl implements Er_Medi_InstService {

	@Autowired
	Er_Medi_InstMapper er_Medi_InstMapper;

	@Override
	public List<Er_BedVO> get_Current_Er_Bed(String gu_Code, String duty_Nm) {

		if (!duty_Nm.equals("")) {
			duty_Nm = "%" + duty_Nm + "%";
		}

		List<Er_BedVO> er_Bed_List = er_Medi_InstMapper.get_Current_Er_Bed(gu_Code, duty_Nm);

		return er_Bed_List;
	}

	@Override
	public List<Er_Medi_InstVO> get_Er_Medi_Inst_Info(String gu_Code, String duty_Nm) {

		if (!duty_Nm.equals("")) {
			duty_Nm = "%" + duty_Nm + "%";
		}

		List<Er_Medi_InstVO> Er_Medi_Inst_List = er_Medi_InstMapper.get_Er_Medi_Inst_Info(gu_Code, duty_Nm);

		return Er_Medi_Inst_List;
	}

	@Override
	public Er_Medi_Inst_Er_BedVO get_Er_Medi_Inst_And_Er_Bed_Info(String hpid) {

		Er_Medi_Inst_Er_BedVO er_Medi_Inst_Er_BedVO = er_Medi_InstMapper.get_Er_Medi_Inst_And_Er_Bed_Info(hpid);
		
		return er_Medi_Inst_Er_BedVO;
	}

}
