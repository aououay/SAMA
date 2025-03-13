package com.pcwk.ehr.disastermessage.mapper;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.disastermessage.domain.DisasterCountVO;
import com.pcwk.ehr.disastermessage.domain.DisasterMessageVO;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface DisasterMessageMapper {

    //재난문자의 모든 정보를 가져오는 쿼리문
    List<DisasterMessageVO> getDisMes(SearchVO searchVO);

    //모든 재난문자의 수를 가져오는 쿼리문
    int getTotalCount(SearchVO searchVO);

    //재난문자의 지역,종류,내용을 단건 검색하는 쿼리문
    DisasterMessageVO getDisMesDetail(Long id);

    //searchWord에 재난빈도수 데이터를 불러옴
    int safetyIndex (SearchVO searchVO);

    //searchWord에 따라 양수량 데이터를 불러옴
    int safetyIndex_EWSF (SearchVO searchVO);

    //searchWord에 따라 재난유형별 빈도수 데이터를 불러옴
    List<DisasterCountVO> item_count (SearchVO searchVO);
    
    // 서울 긴급재난문자 최신 5건 가져오기
    List<DisasterMessageVO> getLatestFiveDisasterMessageForSeoul();
}
