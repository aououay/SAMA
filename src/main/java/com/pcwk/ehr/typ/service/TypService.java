package com.pcwk.ehr.typ.service;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.disastermessage.domain.DisasterMessageVO;
import com.pcwk.ehr.typ.domain.TypVO;
import org.springframework.stereotype.Service;

import java.util.List;

public interface TypService {
    List<TypVO> getTyp(SearchVO searchVO) throws Exception;
}
