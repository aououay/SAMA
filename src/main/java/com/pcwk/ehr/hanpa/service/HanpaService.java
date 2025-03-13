package com.pcwk.ehr.hanpa.service;

import java.util.List;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.hanpa.domain.HanpaVO;

public interface HanpaService {
    List<HanpaVO> getHanpaEmergencies(SearchVO searchVO) throws Exception;
    int getTotalCount(SearchVO searchVO) throws Exception;
}
