package com.pcwk.ehr.insurance.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.insurance.domain.InsVO;
import com.pcwk.ehr.insurance.mapper.InsMapper;

@Service
public class InsServiceImpl implements InsService {

	final Logger log = LoggerFactory.getLogger(getClass());
	
	@Autowired
	InsMapper insMapper;
	

	public InsServiceImpl() {
		super();
		log.info("┌──────────────────────────┐");
		log.info("│ InsServiceImpl()         │");
		log.info("└──────────────────────────┘");
	}

	@Override
	public String getTime() {
		
		return insMapper.getTime();
	}

	@Override
	public List<InsVO> getIns(InsVO insVO) {
		
		return insMapper.getIns(insVO);
	}

	@Override
	public List<InsVO> getGrnt(InsVO insVO) {
		
		return insMapper.getGrnt(insVO);
	}

	@Override
	public List<InsVO> sggNames(InsVO insVO) {

		return insMapper.sggNames(insVO);
	}


}
