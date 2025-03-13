package com.pcwk.ehr.disastermessage.controller;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.cmn.StringUtil;
import com.pcwk.ehr.disastermessage.domain.DisasterCountVO;
import com.pcwk.ehr.disastermessage.domain.DisasterMessageVO;
import com.pcwk.ehr.disastermessage.service.DisasterMessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.sql.SQLException;
import java.util.List;

@Controller
@RequestMapping("disastermessage")
public class DisasterMessageController {

    @Autowired
    private DisasterMessageService disasterMessageService;

    @GetMapping("/view")
    public String showDisasterMessage(Model model,
                                      //pageNo, pageSize 의 기본값을 각각 1,10 으로 설정하여 한페이지당 10줄씩 나오도록 설정
                                      @RequestParam(value = "pageNo", defaultValue = "1") int pageNo,
                                      @RequestParam(value = "pageSize", defaultValue = "10") int pageSize,

                                      //searchDiv를 10(없음),20(지역),30(검색어) , searchWord를 각각 10,"" 값으로 설정하여
                                      // 검색구분,검색어 없이 날짜순으로 올라오도록 설정
                                      @RequestParam(value = "searchDiv", defaultValue = "10") int searchDiv,
                                      @RequestParam(value = "searchWord", defaultValue = "") String searchWord) {

        // 기본 뷰 이름
        String viewName = "disastermessage/disastermessage";

        // SearchVO 객체를 초기화하고 기본 파라미터 값 설정
        SearchVO search = new SearchVO();
        search.setPageNo(pageNo);
        search.setPageSize(pageSize);
        search.setSearchDiv(searchDiv);
        search.setSearchWord(searchWord);

        try {
            // 서비스에서 DisasterMessageVO 데이터를 가져오기
            List<DisasterMessageVO> list = disasterMessageService.getDisMes(search);

            // 모델에 데이터 추가
            model.addAttribute("list", list);

            // 총 글 수 계산
            int totalCnt = 0;
            if (list != null && list.size() > 0) {
                DisasterMessageVO vo = list.get(0);
                totalCnt = vo.getTotalCnt();
            }

            // 총 글 수를 모델에 추가
            model.addAttribute("totalCnt", totalCnt);
            model.addAttribute("search", search);

        } catch (Exception e) {
            e.printStackTrace();
        }
        // 반환할 뷰 이름
        return viewName;
    }

    @RequestMapping("/Detail")
    public String getAnnouncementDetail(@RequestParam("disMesNum") Long disMesNum, Model model) throws Exception {
        String viewName = "disastermessage/disastermessage_detail";

        try {
            // 선택한 재난문자의 disMesNum 으로 단건검색
            DisasterMessageVO disasterMessage = disasterMessageService.getDisMesDetail(disMesNum);

            // 재난문자 단건 정보 모델에 추가
            model.addAttribute("disasterMessage", disasterMessage);
        } catch (Exception e) {
            throw new RuntimeException("상세 정보를 불러오는 중 오류가 발생했습니다.", e);
        }

        return viewName;
    }

    @GetMapping("/safety_Index")
    public String SafetyIndex(Model model,
                              //searchWord를 강남 으로 설정하여 초기값을 강남으로 설정
                              @RequestParam(value = "searchWord", defaultValue = "강남") String searchWord)throws Exception  {
        String viewName = "disastermessage/disastermessage_index";

        SearchVO search = new SearchVO();
        search.setSearchWord(searchWord);

        try {
            //searchWord에 재난빈도수 데이터를 불러옴
            int safetyIndex = disasterMessageService.safetyIndex(search);
            //searchWord에 따라 양수량 데이터를 불러옴
            int safetyIndex_EWSF = disasterMessageService.safetyIndex_EWSF(search);
            //searchWord에 따라 재난유형별 빈도수 데이터를 불러옴
            List <DisasterCountVO> itemCount = disasterMessageService.item_count(search);

            // 모델에 데이터 추가
            model.addAttribute("safetyIndex", safetyIndex);
            model.addAttribute("safetyIndex_EWSF", safetyIndex_EWSF);
            model.addAttribute("searchWord", searchWord);
            model.addAttribute("item_count", itemCount);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return viewName;
    }


}
