package com.pcwk.ehr.insurance.service;

import java.util.List;

import com.pcwk.ehr.insurance.domain.InsVO;

public interface InsService {

	public String getTime();
	
	public List<InsVO> getIns(InsVO insVO);
	
	public List<InsVO> getGrnt(InsVO insVO);
	
	public List<InsVO> sggNames(InsVO insVO);
}
