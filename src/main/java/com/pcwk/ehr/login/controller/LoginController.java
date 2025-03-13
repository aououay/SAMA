package com.pcwk.ehr.login.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.pcwk.ehr.login.service.LoginService;
import com.pcwk.ehr.user.domain.UserVO;

import jakarta.servlet.http.HttpSession;

@Controller
@RequestMapping("login")
public class LoginController {

    final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    LoginService loginService;

    public LoginController() {
        log.info("┌───────────────────────────────────────┐");
        log.info("│ *LoginController()*                   │");
        log.info("└───────────────────────────────────────┘");        
    }

    // 일반 로그인 화면(GET)
    @RequestMapping(value = "login.do", method = RequestMethod.GET)
    public String showLoginPage() {
        return "login";  // /WEB-INF/views/login.jsp
    }

    // 관리자 전용 로그인 화면(GET)
    @RequestMapping(value = "adminLogin.do", method = RequestMethod.GET)
    public String showAdminLoginPage() {
        return "login/adminLogin";  // /WEB-INF/views/login/adminLogin.jsp
    }

    // 일반 로그인 처리(POST)
    @RequestMapping(value = "login.do", method = RequestMethod.POST)
    public String login(@RequestParam("userId") String userId, 
                        @RequestParam("password") String password, 
                        HttpSession session, Model model) throws Exception {
        log.info("┌───────────────────────────────────────┐");
        log.info("│ **login()**                           │");
        log.info("└───────────────────────────────────────┘");

        UserVO user = new UserVO();
        user.setUserId(userId);
        user.setPassword(password);

        int flag = loginService.idPassCheck(user);

        if (flag == 10) {
            model.addAttribute("errorMessage", "아이디를 확인하세요.");
            return "login";
        } else if (flag == 20) {
            model.addAttribute("errorMessage", "비밀번호를 확인하세요.");
            return "login";
        } else if (flag == 30) {
            UserVO outVO = loginService.doSelectOne(user);
            session.setAttribute("user", outVO);
            log.info("userId:{}", userId);
            return "redirect:/index";  // 일반 로그인 성공 시 홈 페이지로 이동
        } else {
            model.addAttribute("errorMessage", "오류가 발생했습니다.");
            return "login";
        }
    }

    // 관리자 로그인 처리(POST)
    @RequestMapping(value = "adminLogin.do", method = RequestMethod.POST)
    public String adminLogin(@RequestParam("userId") String userId, 
                             @RequestParam("password") String password, 
                             HttpSession session, Model model) throws Exception {
        log.info("┌───────────────────────────────────────┐");
        log.info("│ **adminLogin()**                      │");
        log.info("└───────────────────────────────────────┘");

        UserVO user = new UserVO();
        user.setUserId(userId);
        user.setPassword(password);

        int flag = loginService.idPassCheck(user);

        if (flag == 10) {
            model.addAttribute("errorMessage", "아이디를 확인하세요.");
            return "login/adminLogin";
        } else if (flag == 20) {
            model.addAttribute("errorMessage", "비밀번호를 확인하세요.");
            return "login/adminLogin";
        } else if (flag == 30) {
            UserVO outVO = loginService.doSelectOne(user);
            log.info("Retrieved isAdmin value: {}", outVO.getIsAdmin());

            // 관리자 로그인 체크 (isAdmin 필드가 "Y" 인지 확인)
            String isAdmin = outVO.getIsAdmin();
            if (isAdmin == null || !"Y".equalsIgnoreCase(isAdmin.trim())) {
                model.addAttribute("errorMessage", "관리자 권한이 없습니다.");
                return "login/adminLogin";
            }

            // 관리자 로그인 성공
            session.setAttribute("user", outVO);
            log.info("Admin userId: {}", userId);
            return "redirect:/main_user/doRetrieve.do";  // 관리자 로그인 성공 시 관리자 페이지로 이동
        } else {
            model.addAttribute("errorMessage", "오류가 발생했습니다.");
            return "login/adminLogin";
        }
    }

    // 비밀번호 찾기 페이지(GET)
    @RequestMapping(value = "changePassword.do", method = RequestMethod.GET)
    public String pwFindIndex() {
        log.debug("┌───────────────────────────────────────┐");
        log.debug("│ **pwFindIndex()**                     │");
        log.debug("└───────────────────────────────────────┘");
        return "login/pw_find";  // /WEB-INF/views/login/pw_find.jsp
    }
    
    // 로그아웃 처리(GET)
    @RequestMapping(value = "logout.do", method = RequestMethod.GET)
    public String logout(HttpSession session) {
        session.invalidate();
        return "redirect:/login/login.do";
    }
    
}
