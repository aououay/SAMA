package com.pcwk.ehr.mypage.controller;

import jakarta.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired; 
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;
import com.pcwk.ehr.cmn.MessageVO;
import com.pcwk.ehr.user.domain.UserVO;
import com.pcwk.ehr.user.service.UserService;

@Controller
public class MypageController {
    final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private UserService userService;
    
    // 마이페이지 메인 (정보 조회)
    @GetMapping("/mypage")
    public String showMyPage(HttpSession session, Model model) throws Exception {
        UserVO sessionUser = (UserVO) session.getAttribute("user");
        if (sessionUser == null) {
            log.warn("세션에 사용자 정보가 없습니다. 로그인 페이지로 이동합니다.");
            return "redirect:/login/login.do";
        }
        UserVO user = userService.doSelectOne(sessionUser);
        model.addAttribute("user", user);
        return "mypage";  // 예: /WEB-INF/views/mypage.jsp
    }
    
    // 마이페이지 수정 폼 보여주기
    @GetMapping("/mypage/edit")
    public String showEditPage(HttpSession session, Model model) throws Exception {
        UserVO sessionUser = (UserVO) session.getAttribute("user");
        if (sessionUser == null) {
            return "redirect:/login/login.do";
        }
        UserVO user = userService.doSelectOne(sessionUser);
        model.addAttribute("user", user);
        return "mypage_edit";  // 위의 JSP 파일
    }
    
    @PostMapping("/mypage/update")
    public String updateUser(UserVO user, HttpSession session, Model model) throws Exception {
        // 로그인 여부 확인
        UserVO sessionUser = (UserVO) session.getAttribute("user");
        if (sessionUser == null) {
            return "redirect:/login/login.do";
        }
        
        // 보안을 위해 세션의 회원ID를 강제로 적용 (폼 조작 방지)
        user.setUserId(sessionUser.getUserId());
        
        // 비밀번호 변경: 입력하지 않은 경우 기존 비밀번호 유지
        if (user.getPassword() == null || user.getPassword().trim().isEmpty()) {
            user.setPassword(sessionUser.getPassword());
        }
        
        // 회원 정보 수정 처리 (업데이트 성공 시 1 반환한다고 가정)
        int flag = userService.doUpdate(user);
        if (flag != 1) {
            model.addAttribute("errorMessage", "회원 정보 수정에 실패했습니다.");
            return "mypage_edit";
        }
        
        // 수정 성공 시, 세션 정보 업데이트
        session.setAttribute("user", user);
        
     // 성공 메시지를 모델에 추가 후 수정 폼(JSP)으로 포워딩
        model.addAttribute("successMessage", "정보수정이 완료 되었습니다");
        return "mypage";
    }
    


}
