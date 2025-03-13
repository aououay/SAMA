package com.pcwk.ehr.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.pcwk.ehr.er_medi_inst.domain.Er_BedVO;
import com.pcwk.ehr.er_medi_inst.domain.Er_Medi_InstVO;
import com.pcwk.ehr.er_medi_inst.domain.Er_Medi_Inst_Er_BedVO;

@Mapper
public interface Er_Medi_InstMapper {

	/**
	 * 응급의료기관의 현재 응급실 가용병상 정보 조회
	 * 
	 * @return Weather_CurrentVO
	 */
	public List<Er_BedVO> get_Current_Er_Bed(@Param("gu_Code") String gu_Code, @Param("duty_Nm") String duty_Nm);
	
	/**
	 * 응급의료기관 정보 조회
	 * @return
	 */
	public List<Er_Medi_InstVO> get_Er_Medi_Inst_Info(@Param("gu_Code") String gu_Code, @Param("duty_Nm") String duty_Nm);
	
	/**
	 * 응급의료기관 정보와 응급실 가용병상 정보 조회
	 * @return
	 */
	public Er_Medi_Inst_Er_BedVO get_Er_Medi_Inst_And_Er_Bed_Info(@Param("hpid") String hpid);

}
