package com.pcwk.ehr.faclt.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.faclt.domain.FacltVO;
import com.pcwk.ehr.faclt.mapper.FacltMapper;

@Service
public class FacltServiceImpl implements FacltService {

	@Autowired
	FacltMapper facltMapper;

	@Override
	public List<FacltVO> getFaclts(SearchVO searchVO) throws Exception {
		// 페이징 처리
		searchVO.setPaging();
		return facltMapper.getFaclts(searchVO);
	}

	@Override
	public int getTotalCount(SearchVO searchVO) throws Exception {
		return facltMapper.getTotalCount(searchVO);
	}



}
