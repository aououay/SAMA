package com.pcwk.ehr.insurance.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.pcwk.ehr.insurance.domain.InsVO;

@Mapper
public interface InsMapper {

	public String getTime();
	
	List<InsVO> getIns(InsVO insVO);
	
	List<InsVO> getGrnt(InsVO insVO);
	
	List<InsVO> sggNames(InsVO insVO);
}
