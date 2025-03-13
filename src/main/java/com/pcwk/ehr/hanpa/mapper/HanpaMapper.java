package com.pcwk.ehr.hanpa.mapper;

import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.hanpa.domain.HanpaVO;

@Mapper
public interface HanpaMapper {

    // 한파 응급시설 목록 조회
    List<HanpaVO> getHanpaEmergencies(SearchVO searchVO);

    // 한파 응급시설 개수 조회
    int getTotalCount(SearchVO searchVO);
}
