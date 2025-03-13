package com.pcwk.ehr.ewsf.mapper;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.disastermessage.domain.DisasterMessageVO;
import com.pcwk.ehr.ewsf.domain.EwsfVO;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface EwsfMapper {
    List<EwsfVO> getEWSF();
}
