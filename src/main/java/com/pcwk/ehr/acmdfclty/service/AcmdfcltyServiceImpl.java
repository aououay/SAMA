package com.pcwk.ehr.acmdfclty.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.acmdfclty.domain.AcmdfcltyVO;
import com.pcwk.ehr.acmdfclty.mapper.AcmdfcltyMapper;

@Service
public class AcmdfcltyServiceImpl implements AcmdfcltyService {

    @Autowired
    AcmdfcltyMapper acmdfcltyMapper;

    @Override
    public List<AcmdfcltyVO> getAcmdfcltyList(SearchVO searchVO) throws Exception {
        // 페이징 처리
        searchVO.setPaging();
        return acmdfcltyMapper.getAcmdfcltyList(searchVO);
    }

    @Override
    public int getTotalCount(SearchVO searchVO) throws Exception {
        return acmdfcltyMapper.getTotalCount(searchVO);
    }
}
