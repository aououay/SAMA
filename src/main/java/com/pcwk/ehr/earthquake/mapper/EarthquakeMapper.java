package com.pcwk.ehr.earthquake.mapper;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.disastermessage.domain.DisasterMessageVO;
import com.pcwk.ehr.earthquake.domain.EarthquakeVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface EarthquakeMapper {

    //우리나라에서 일어난 지진의 데이터를 EarthquakeVO 타입으로 불러옴
    List<EarthquakeVO> getEQK (SearchVO searchVO);

    //우리나라에서 일어난 지진중 규모 3.0이상의 지진을 규모별로 분류하여 몇회씩 있었는지 가져옴
    List<EarthquakeVO> eqkByYear ();

    //우리나라에서 일어난 지진중 선택한 연도의 규모 위도 경도값을 가져옴
    List<EarthquakeVO> eqkOccurrence (SearchVO searchVO);

    //선택한 연도에 2.0이상의 지진을 규모별로 분류하여 몇회씩 있었는지 데이터를 가져옴
    List<EarthquakeVO> eqkByYearAll (SearchVO searchVO);
}
