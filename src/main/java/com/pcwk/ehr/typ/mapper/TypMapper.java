package com.pcwk.ehr.typ.mapper;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.typ.domain.TypVO;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface TypMapper {
    // 선택한 연도의 태풍 데이터를 불러옴
    List<TypVO> getTyp(SearchVO searchVO) throws Exception;
}
