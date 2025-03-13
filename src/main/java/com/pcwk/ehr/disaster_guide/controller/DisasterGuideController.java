package com.pcwk.ehr.disaster_guide.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("disasterguide")
public class DisasterGuideController {

    @GetMapping("/finedust")
    public String fineDust() {
        String viewName = "disasters_guide/natural_disasters_guide";
        return viewName;
    }

    @GetMapping("/typhoon")
    public String typhoon() {
        String viewName = "disasters_guide/typhoon_guide";
        return viewName;
    }

    @GetMapping("/heatwave")
    public String heatwave() {
        String viewName = "disasters_guide/heatwave_guide";
        return viewName;
    }

    @GetMapping("/heavyrain")
    public String heavyRain() {
        String viewName = "disasters_guide/heavyrain_guide";
        return viewName;
    }

    @GetMapping("/flood")
    public String flood() {
        String viewName = "disasters_guide/flood_guide";
        return viewName;
    }

    @GetMapping("/coldwave")
    public String coldWave() {
        String viewName = "disasters_guide/coldwave_guide";
        return viewName;
    }

    @GetMapping("/snowstorm")
    public String snowstorm() {
        String viewName = "disasters_guide/snowstorm_guide";
        return viewName;
    }


    @GetMapping("/fire")
    public String fire(){
        String viewName = "disasters_guide/social_disaster_guide";
        return viewName;
    }

    @GetMapping("/forestfires")
    public String forestfires(){
        String viewName = "disasters_guide/forestfires_guide";
        return viewName;
    }

    @GetMapping("/electricity_gas_accident")
    public String electricity_gas_accident(){
        String viewName = "disasters_guide/electricity_gas_accident_guide";
        return viewName;
    }

    @GetMapping("/car_accident")
    public String car_accident(){
        String viewName = "disasters_guide/car_accident_guide";
        return viewName;
    }

}
