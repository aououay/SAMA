package com.pcwk.ehr.faclt.controller;

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
import com.pcwk.ehr.faclt.domain.FacltVO;
import com.pcwk.ehr.faclt.service.FacltService;

@Controller
@RequestMapping("faclt")
public class FacltController {
    
    private static final Logger LOG = LoggerFactory.getLogger(FacltController.class);

    @Autowired
    private FacltService facltService;

    @GetMapping("/mapPopup")
    public String showFacltMapPopup(@RequestParam(value = "name", required = false) String name,
                                    @RequestParam(value = "address", required = false) String address,
                                    @RequestParam(value = "lat", required = false) Double lat,
                                    @RequestParam(value = "lng", required = false) Double lng,
                                    @RequestParam(value = "opnYn", required = false) String opnYn,
                                    @RequestParam(value = "mngInstNm", required = false) String mngInstNm,
                                    @RequestParam(value = "ortmUtlzType", required = false) String ortmUtlzType,
                                    @RequestParam(value = "mngInstTelno", required = false) String mngInstTelno,
                                    @RequestParam(value = "fcltCd", required = false) String fcltCd,
                                    Model model) {

        model.addAttribute("name", name != null ? name : "이름 없음");
        model.addAttribute("address", address != null ? address : "주소 정보 없음");
        model.addAttribute("lat", lat);
        model.addAttribute("lng", lng);
        model.addAttribute("opnYn", opnYn != null ? opnYn : "정보 없음");
        model.addAttribute("mngInstNm", mngInstNm != null ? mngInstNm : "정보 없음");
        model.addAttribute("ortmUtlzType", ortmUtlzType != null ? ortmUtlzType : "정보 없음");
        model.addAttribute("mngInstTelno", mngInstTelno != null ? mngInstTelno : "정보 없음");
        model.addAttribute("fcltCd", fcltCd != null ? fcltCd : "정보 없음");

        return "facltmap/facltMap";
    }


    @GetMapping("/view")
    public String showFacltList(Model model,
                                @RequestParam(value = "pageNo", defaultValue = "1") int pageNo,
                                @RequestParam(value = "pageSize", defaultValue = "10") int pageSize,
                                @RequestParam(value = "searchDiv", defaultValue = "10") int searchDiv,
                                @RequestParam(value = "searchWord", defaultValue = "") String searchWord) {

        String viewName = "faclt/faclt";

        // SearchVO 객체 초기화
        SearchVO search = new SearchVO();
        search.setPageNo(pageNo);
        search.setPageSize(pageSize);
        search.setSearchDiv(searchDiv);
        search.setSearchWord(searchWord);

        try {
            // 서비스에서 데이터 가져오기
            List<FacltVO> list = facltService.getFaclts(search);
            model.addAttribute("list", list);

            // 총 글 수 계산
            int totalCnt = 0;
            if (list != null && !list.isEmpty()) {
                totalCnt = list.get(0).getTotalCnt();
            }

            model.addAttribute("totalCnt", totalCnt);
            model.addAttribute("search", search);

        } catch (Exception e) {
            LOG.error("시설 리스트 조회 중 오류 발생", e);
        }
        return viewName;
    }
}
