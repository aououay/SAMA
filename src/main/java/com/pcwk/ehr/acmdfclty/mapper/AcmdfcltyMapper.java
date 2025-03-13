package com.pcwk.ehr.acmdfclty.mapper;

import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.acmdfclty.domain.AcmdfcltyVO;

@Mapper
public interface AcmdfcltyMapper {
    List<AcmdfcltyVO> getAcmdfcltyList(SearchVO searchVO);
    int getTotalCount(SearchVO searchVO);
}
