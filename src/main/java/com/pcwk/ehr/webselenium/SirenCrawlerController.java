package com.pcwk.ehr.webselenium;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class SirenCrawlerController {

    @GetMapping("/siren")
    public String sirenData() {
       return "siren/sirenWeb";
        
    }
}
