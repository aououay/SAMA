package com.pcwk.ehr.rainfall;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.repository.query.Param;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/rainfall")
public class RainfallController {
    private final RainfallService rainfallService;

    public RainfallController(RainfallService rainfallService) {
        this.rainfallService = rainfallService;
    }

    // JSP 페이지 반환
    @GetMapping("view")
    public String showRainfallPage(Model model) {

        List<Rainfall> allStations = rainfallService.getAllRainfallStations();
        List<Rainfall> guList = rainfallService.getGuList(); // 자치구 목록
        List<Rainfall> guRainfall = rainfallService.getGuRainfall();
        List<Rainfall> warnings = rainfallService.getRainfallWarnings(); 
        System.out.println("반환할 데이터: " + guRainfall);
        System.out.println("자치구 목록: " + guList);
        if (warnings == null || warnings.isEmpty()) {
            warnings = new ArrayList<>();
        }
        System.out.println("호우 특보 데이터: " + warnings);
        model.addAttribute("allRainfallStations", allStations);
        model.addAttribute("guRainfall", guRainfall);
        model.addAttribute("guList", guList);
        model.addAttribute("warnings", warnings);
        return "rainfall/rainfall";
    }
    
    @GetMapping("/gurainfall")
    @ResponseBody
    public ResponseEntity<List<Rainfall>> getGuRainfall() {
        List<Rainfall> rainfallData = rainfallService.getGuRainfall();
        System.out.println("강수량 데이터 확인: " + rainfallData);
        return ResponseEntity.ok(rainfallData);
    }
    
    @GetMapping("/warnings")
    @ResponseBody
    public List<Rainfall> getRainfallWarnings() {
        return rainfallService.getRainfallWarnings();
    }



    // AJAX 요청에 대한 응답
    @GetMapping("/recent/{rfObsCd}")
    @ResponseBody
    public List<Rainfall> getRecentRainfall(@PathVariable("rfObsCd") int rfObsCd,Model model) {
    	
    	List<Rainfall> rainfallData = rainfallService.getRecentRainfall(rfObsCd);
    	
    	
        System.out.println("전달할 데이터: " + rainfallData);
        model.addAttribute("recentRainfall", rainfallData);
    	
    	return rainfallData;
    }
    
    
   

}
