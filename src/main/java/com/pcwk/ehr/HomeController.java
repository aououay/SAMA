package com.pcwk.ehr;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired; 
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import com.pcwk.ehr.user.service.UserService;

@Controller
public class HomeController {
    final Logger log = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private UserService userService;
    
    // 메인페이지
    @GetMapping("/main")
    public String home() {
        log.info("메인페이지 접근");
        return "redirect:/index"; 
    }
    
    // 로그아웃: 세션 무효화 후 로그인 페이지로 리다이렉트
    @GetMapping("/logout")
    public String logout(HttpServletRequest request) {
        HttpSession session = request.getSession(false);
        if(session != null) {
            session.invalidate();
        }
        log.info("로그아웃 처리 완료");
        return "redirect:/index"; // 로그인 페이지로 리다이렉트
    }
}
