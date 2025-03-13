package com.pcwk.ehr.earthquake.service;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.earthquake.domain.EarthquakeVO;


import java.util.List;

public interface EarthquakeService {

    List<EarthquakeVO> getEQK(SearchVO searchVO) throws Exception;
    List<EarthquakeVO> eqkByYear ();
    List<EarthquakeVO> eqkOccurrence (SearchVO searchVO) throws Exception;
    List<EarthquakeVO> eqkByYearAll (SearchVO searchVO) throws Exception;
}
