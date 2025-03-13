package com.pcwk.ehr.acmdfclty.service;

import java.util.List;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.acmdfclty.domain.AcmdfcltyVO;

public interface AcmdfcltyService {
    List<AcmdfcltyVO> getAcmdfcltyList(SearchVO searchVO) throws Exception;
    int getTotalCount(SearchVO searchVO) throws Exception;
}
