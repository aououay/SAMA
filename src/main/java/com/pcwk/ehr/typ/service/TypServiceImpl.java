package com.pcwk.ehr.typ.service;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.typ.domain.TypVO;
import com.pcwk.ehr.typ.mapper.TypMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TypServiceImpl implements TypService {

    @Autowired
    TypMapper typMapper;

    @Override
    public List<TypVO> getTyp(SearchVO searchVO) throws Exception {
        return typMapper.getTyp(searchVO);
    }
}
