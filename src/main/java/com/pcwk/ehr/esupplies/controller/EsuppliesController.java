package com.pcwk.ehr.esupplies.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class EsuppliesController {
    // 홈 페이지 요청 처리 (GET 요청)
    @RequestMapping("/esupplies")
    public String showEsuppliesPage() {
        return "esupplies/esupplies"; 
    }
}
