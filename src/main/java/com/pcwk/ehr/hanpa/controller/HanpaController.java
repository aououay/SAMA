package com.pcwk.ehr.hanpa.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.hanpa.domain.HanpaVO;
import com.pcwk.ehr.hanpa.service.HanpaService;

@Controller
@RequestMapping("hanpa")
public class HanpaController {
    
    private static final Logger LOG = LoggerFactory.getLogger(HanpaController.class);

    @Autowired
    private HanpaService hanpaService;

    @GetMapping("/view")
    public String showHanpaList(Model model,
                                @RequestParam(value = "pageNo", defaultValue = "1") int pageNo,
                                @RequestParam(value = "pageSize", defaultValue = "10") int pageSize,
                                @RequestParam(value = "searchDiv", defaultValue = "10") int searchDiv,
                                @RequestParam(value = "searchWord", defaultValue = "") String searchWord) {

        String viewName = "hanpa/hanpa";

        // SearchVO 객체 초기화
        SearchVO search = new SearchVO();
        search.setPageNo(pageNo);
        search.setPageSize(pageSize);
        search.setSearchDiv(searchDiv);
        search.setSearchWord(searchWord);

        try {
            // 서비스에서 데이터 가져오기
            List<HanpaVO> list = hanpaService.getHanpaEmergencies(search);
            model.addAttribute("list", list);

            // 총 글 수 계산
            int totalCnt = 0;
            if (list != null && !list.isEmpty()) {
                totalCnt = list.get(0).getTotalCnt();
            }

            model.addAttribute("totalCnt", totalCnt);
            model.addAttribute("search", search);

        } catch (Exception e) {
            LOG.error("한파 대피소 리스트 조회 중 오류 발생", e);
        }
        return viewName;
    }
}
