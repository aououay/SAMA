package com.pcwk.ehr.acmdfclty.controller;

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
import com.pcwk.ehr.acmdfclty.domain.AcmdfcltyVO;
import com.pcwk.ehr.acmdfclty.service.AcmdfcltyService;

@Controller
@RequestMapping("acmdfclty")
public class AcmdfcltyController {

    private static final Logger LOG = LoggerFactory.getLogger(AcmdfcltyController.class);

    @Autowired
    private AcmdfcltyService acmdfcltyService;

    @GetMapping("/mapPopup")
    public String showAcmdfcltyMapPopup(@RequestParam(value = "name", required = false) String name,
                                        @RequestParam(value = "address", required = false) String address,
                                        @RequestParam(value = "lat", required = false) Double lat,
                                        @RequestParam(value = "lng", required = false) Double lng,
                                        @RequestParam(value = "capacity", required = false) Integer capacity,
                                        @RequestParam(value = "facilityArea", required = false) Double facilityArea,
                                        @RequestParam(value = "useSeCd", required = false) String useSeCd,
                                        @RequestParam(value = "managementOffice", required = false) String managementOffice,
                                        Model model) {

        model.addAttribute("name", name != null ? name : "이름 없음");
        model.addAttribute("address", address != null ? address : "주소 정보 없음");
        model.addAttribute("lat", lat);
        model.addAttribute("lng", lng);
        model.addAttribute("capacity", capacity != null ? capacity : "정보 없음");
        model.addAttribute("facilityArea", facilityArea != null ? facilityArea : "정보 없음");
        model.addAttribute("useSeCd", useSeCd != null ? useSeCd : "정보 없음");
        model.addAttribute("managementOffice", managementOffice != null ? managementOffice : "정보 없음");

        return "acmdfcltymap/acmdfcltyMap";
    }

    @GetMapping("/view")
    public String showAcmdfcltyList(Model model,
                                    @RequestParam(value = "pageNo", defaultValue = "1") int pageNo,
                                    @RequestParam(value = "pageSize", defaultValue = "10") int pageSize,
                                    @RequestParam(value = "searchDiv", defaultValue = "10") int searchDiv,
                                    @RequestParam(value = "searchWord", defaultValue = "") String searchWord) {

        String viewName = "acmdfclty/acmdfclty";

        // SearchVO 객체 초기화
        SearchVO search = new SearchVO();
        search.setPageNo(pageNo);
        search.setPageSize(pageSize);
        search.setSearchDiv(searchDiv);
        search.setSearchWord(searchWord);

        try {
            // 서비스에서 데이터 가져오기
            List<AcmdfcltyVO> list = acmdfcltyService.getAcmdfcltyList(search);
            model.addAttribute("list", list);

            // 총 개수 계산
            int totalCnt = 0;
            if (list != null && !list.isEmpty()) {
                totalCnt = list.get(0).getTotalCnt();
            }

            model.addAttribute("totalCnt", totalCnt);
            model.addAttribute("search", search);

        } catch (Exception e) {
            LOG.error("이재민 수용시설 리스트 조회 중 오류 발생", e);
        }
        return viewName;
    }
}
