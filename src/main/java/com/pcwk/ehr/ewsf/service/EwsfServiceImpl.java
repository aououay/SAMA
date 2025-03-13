package com.pcwk.ehr.ewsf.service;

import com.pcwk.ehr.ewsf.domain.EwsfVO;
import com.pcwk.ehr.ewsf.mapper.EwsfMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EwsfServiceImpl implements EwsfService {

    @Autowired
    EwsfMapper ewsfMapper;

    @Override
    public List<EwsfVO> getEWSF() {
        return ewsfMapper.getEWSF();
    }
}
