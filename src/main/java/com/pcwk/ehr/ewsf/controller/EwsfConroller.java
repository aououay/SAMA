package com.pcwk.ehr.ewsf.controller;

import com.pcwk.ehr.ewsf.domain.EwsfVO;
import com.pcwk.ehr.ewsf.service.EwsfService;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Controller
@RequestMapping("ewsf")
public class EwsfConroller {

    @Autowired
    EwsfService ewsfService;

    @GetMapping("/view")
    public String showEwsf(Model model) {
        String viewName = "ewsf/ewsf";

        try {
            List<EwsfVO> list = ewsfService.getEWSF();
            model.addAttribute("list", list);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return viewName;
    }
}
