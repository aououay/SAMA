package com.pcwk.ehr.faclt.service;

import java.util.List;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.faclt.domain.FacltVO;

public interface FacltService {
    List<FacltVO> getFaclts(SearchVO searchVO) throws Exception;
    int getTotalCount(SearchVO searchVO) throws Exception;
}
