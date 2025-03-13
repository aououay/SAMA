package com.pcwk.ehr.disastermessage.service;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.disastermessage.domain.DisasterCountVO;
import com.pcwk.ehr.disastermessage.domain.DisasterMessageVO;

import java.util.List;

public interface DisasterMessageService {

	List<DisasterMessageVO> getDisMes(SearchVO searchVO) throws Exception;

	int getTotalCount(SearchVO searchVO) throws Exception;

	DisasterMessageVO getDisMesDetail(Long id) throws Exception;

	int safetyIndex(SearchVO searchVO) throws Exception;

	int safetyIndex_EWSF(SearchVO searchVO) throws Exception;

	List<DisasterCountVO> item_count(SearchVO searchVO) throws Exception;

	List<DisasterMessageVO> getLatestFiveDisasterMessageForSeoul();
}
