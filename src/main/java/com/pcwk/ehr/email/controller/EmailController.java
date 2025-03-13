package com.pcwk.ehr.email.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import com.pcwk.ehr.cmn.MessageVO;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.email.domain.EmailVO;
import com.pcwk.ehr.email.service.EmailService;
import com.pcwk.ehr.user.domain.UserVO;
import com.pcwk.ehr.user.service.UserService;

@Controller
@RequestMapping("/email")
public class EmailController {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    EmailService emailService;
    
    @Autowired
    private UserService userService;
    
    
    public EmailController() {
		super();
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **EmailController()**                  │");
		log.info("└───────────────────────────────────────┘");
	}

    // 인증번호 발송
    @PostMapping("/sendAuthCode.do")
    public ResponseEntity<MessageVO> sendAuthCode(@RequestBody EmailVO emailVO) {
        log.info("sendAuthCode called with EmailVO: {}", emailVO); 
        try {
        	emailService.doSave(emailVO.getEmail());
            emailService.send_Auth_Code(emailVO);
            return new ResponseEntity<>(new MessageVO(1, "인증 코드가 전송되었습니다."), HttpStatus.OK);
        } catch (Exception e) {
            log.info("인증 코드 전송 실패: {}", e.getMessage(), e);
            return new ResponseEntity<>(new MessageVO(0, "인증 코드 전송에 실패했습니다: " + e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    //인증번호 확인
    @PostMapping("/verifyAuthCode.do")
    public ResponseEntity<MessageVO> verifyAuthCode(@RequestBody EmailVO emailVO) {
        log.info("verifyAuthCode called with EmailVO: {}", emailVO); 
        try {
            int result = emailService.verifyAuthCode(emailVO);
            if (result == 1) {
                emailService.doUpdate_reg_stat_1(emailVO.getEmail());
                return new ResponseEntity<>(new MessageVO(1, "인증 코드가 확인되었습니다. 이메일이 인증되었습니다."), HttpStatus.OK);
            } else {
                return new ResponseEntity<>(new MessageVO(0, "인증 코드가 유효하지 않습니다."), HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            log.info("인증 코드 확인 실패: {}", e.getMessage(), e);
            return new ResponseEntity<>(new MessageVO(0, "인증 코드 확인에 실패했습니다: " + e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    
    // 비밀번호 변경
    @PostMapping("/changePassword.do")
    public ResponseEntity<MessageVO> changePassword(@RequestBody UserVO userVO) {
    	log.info(userVO.getEmail());
    	log.info(userVO.getNewPassword());
        try {
            int result = emailService.changePassword(userVO.getEmail(), userVO.getNewPassword());
            if (result > 0) {
                return new ResponseEntity<>(new MessageVO(1, "비밀번호가 변경되었습니다."), HttpStatus.OK);
            } else {
                return new ResponseEntity<>(new MessageVO(0, "비밀번호 변경 실패."), HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
        	log.info("비밀번호 변경 중 오류 발생: {}", e.getMessage(), e);
            return new ResponseEntity<>(new MessageVO(0, "비밀번호 변경 중 오류가 발생했습니다."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
          
    /**
     * 아이디 찾기 기능
     * - 클라이언트가 EmailVO(JSON 형식)로 이메일과 인증번호를 보냄.
     * - 이메일 서비스로 인증번호를 확인하여, 인증이 성공하면,
     *   기존 회원 조회 기능을 활용해 해당 이메일로 등록된 회원 정보를 검색.
     * - 검색 결과가 있으면 회원의 아이디(userId)를 추출하여 응답 메시지로 반환.
     */
    @PostMapping("/findUserId.do")
    public ResponseEntity<MessageVO> findUserId(@RequestBody EmailVO emailVO) {
        log.info("findUserId 호출됨 - EmailVO: {}", emailVO);

        if (emailVO.getEmail() == null || emailVO.getEmail().trim().isEmpty() ||
            emailVO.getAuth_code() == null || emailVO.getAuth_code().trim().isEmpty()) {
            return new ResponseEntity<>(new MessageVO(0, "이메일과 인증번호를 입력해주세요."), HttpStatus.BAD_REQUEST);
        }
        
        try {
            // 인증번호 검증
            int authResult = emailService.verifyAuthCode(emailVO);
            if (authResult != 1) {
                log.warn("인증 코드가 유효하지 않습니다: {}", emailVO.getAuth_code());
                return new ResponseEntity<>(new MessageVO(0, "인증 코드가 유효하지 않습니다."), HttpStatus.BAD_REQUEST);
            }
            
            // 이메일을 기준으로 회원 정보를 조회 (새로운 메서드 사용)
            UserVO outVO = userService.findUserByEmail(emailVO.getEmail());
            log.info("outVO: {}", outVO);
            
            if (outVO != null) {
                String message = "회원님의 아이디는: " + outVO.getUserId();
                return new ResponseEntity<>(new MessageVO(1, message), HttpStatus.OK);
            } else {
                return new ResponseEntity<>(new MessageVO(0, "해당 이메일로 등록된 회원이 없습니다."), HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            log.error("아이디 찾기 중 오류 발생: {}", e.getMessage(), e);
            return new ResponseEntity<>(new MessageVO(0, "아이디 찾기 중 오류 발생: " + e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    
    @GetMapping("/id-find")
    public String showIdFindPage() {
        return "login/id_find";
    }

}
