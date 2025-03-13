package com.pcwk.ehr.faclt.mapper;

import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.faclt.domain.FacltVO;

@Mapper
public interface FacltMapper {
    List<FacltVO> getFaclts(SearchVO searchVO);
    int getTotalCount(SearchVO searchVO);
}
