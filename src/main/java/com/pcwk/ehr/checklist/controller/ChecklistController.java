package com.pcwk.ehr.checklist.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ChecklistController {

    // 홈 페이지 요청 처리 (GET 요청)
    @RequestMapping("/checklist")
    public String showChecklistPage() {
        return "checklist/checklist"; 
    }
}
