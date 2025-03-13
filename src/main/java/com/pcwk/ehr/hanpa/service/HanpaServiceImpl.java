package com.pcwk.ehr.hanpa.service;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.hanpa.domain.HanpaVO;
import com.pcwk.ehr.hanpa.mapper.HanpaMapper;

@Service
public class HanpaServiceImpl implements HanpaService {

    @Autowired
    HanpaMapper hanpaMapper;

    @Override
    public List<HanpaVO> getHanpaEmergencies(SearchVO searchVO) throws Exception {
        // 페이징 처리
        searchVO.setPaging();
        return hanpaMapper.getHanpaEmergencies(searchVO);
    }

    @Override
    public int getTotalCount(SearchVO searchVO) throws Exception {
        return hanpaMapper.getTotalCount(searchVO);
    }
}
