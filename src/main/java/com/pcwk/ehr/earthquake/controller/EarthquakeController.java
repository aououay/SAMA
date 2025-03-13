package com.pcwk.ehr.earthquake.controller;

import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.earthquake.domain.EarthquakeVO;
import com.pcwk.ehr.earthquake.service.EarthquakeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
@RequestMapping("earthquake")
public class EarthquakeController {

    @Autowired
    private EarthquakeService earthquakeService;

    @GetMapping("view")
    public String viewEarthquake(Model model,
                                 // searchWord(연도), searchDiv( (10)최신순, (20)규모순 ) 기본값을 각각 2025, 와 10 으로 줌
                                 @RequestParam(value = "searchWord", defaultValue = "2025")String searchWord,
                                 @RequestParam(value = "searchDiv", defaultValue = "10") int searchDiv) throws Exception {

        String viewName = "earthquake/earthquake";

        // 연도와 최신순 규모순으로 분류하는 첫번째 지도에서의 SearchVO 객체를 search로 생성
        SearchVO search = new SearchVO();
        search.setSearchWord(searchWord);
        search.setSearchDiv(searchDiv);

        // 연도로 분류하는 두번째 지도에서의 searchVO 객체를 search1으로 생성
        SearchVO search1 = new SearchVO();
        search1.setSearchWord(searchWord);

        try {
            //우리나라에서 일어난 지진의 데이터를 EarthquakeVO 타입으로 불러옴
            List<EarthquakeVO> list = earthquakeService.getEQK(search);

            //우리나라에서 일어난 지진중 선택한 연도의 규모 위도 경도값을 가져옴
            List<EarthquakeVO> eqkOccurrence = earthquakeService.eqkOccurrence(search1);

            //우리나라에서 일어난 지진중 규모 3.0이상의 지진을 규모별로 분류하여 몇회씩 있었는지 가져옴
            List<EarthquakeVO> eqkByYear = earthquakeService.eqkByYear();

            //선택한 연도에 2.0이상의 지진을 규모별로 분류하여 몇회씩 있었는지 데이터를 가져옴
            List<EarthquakeVO> eqkByYearAll = earthquakeService.eqkByYearAll(search1);

            // 모델에 데이터 추가
            model.addAttribute("list", list);
            model.addAttribute("eqkOccurrence", eqkOccurrence);
            model.addAttribute("eqkByYear", eqkByYear);
            model.addAttribute("eqkByYearAll", eqkByYearAll);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return viewName;
    }


}
