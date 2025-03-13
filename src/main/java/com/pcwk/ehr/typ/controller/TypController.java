package com.pcwk.ehr.typ.controller;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.typ.domain.TypVO;
import com.pcwk.ehr.typ.service.TypService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
@RequestMapping("typ")
public class TypController {

    @Autowired
    TypService typService;


    @GetMapping("view")
    public String showTYP(Model model,
                          // searchWord(연도), searchDiv( (10)전체, (20)한반도 상률 (30)한반도에 직접 영향 ) 기본값을 각각 2023, 와 10 으로 줌
                          @RequestParam(value = "searchDiv", defaultValue = "10") int searchDiv,
                          @RequestParam(value = "searchWord", defaultValue = "2023")String searchWord) {
        String viewName = "typ/typ";

        try {
            // 연도와 최신순 규모순으로 분류하는 첫번째 지도에서의 SearchVO 객체를 search로 생성
            SearchVO searchVO = new SearchVO();
            searchVO.setSearchDiv(searchDiv);
            searchVO.setSearchWord(searchWord);

            
            // 선택한 연도의 태풍 데이터를 불러옴
            List<TypVO> typ = typService.getTyp(searchVO);
            
            // 모델에 데이터 추가 
            model.addAttribute("typ", typ);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        return viewName;
    }

}
