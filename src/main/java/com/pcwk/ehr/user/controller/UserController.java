package com.pcwk.ehr.user.controller;

import com.pcwk.ehr.cmn.StringUtil;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;
import com.pcwk.ehr.cmn.DataNotFoundException;
import com.pcwk.ehr.cmn.MessageVO;
import com.pcwk.ehr.cmn.SearchBoardVO;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.user.domain.UserVO;
import com.pcwk.ehr.user.service.UserService;

import jakarta.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("main_user")
public class UserController {
	final Logger log = LoggerFactory.getLogger(getClass());
	
	@Autowired
	UserService userService;

	
	public UserController() {
		super();
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **UserController()**                  │");
		log.info("└───────────────────────────────────────┘");
	}
	
	@GetMapping("/generateUserCode.do")
	@ResponseBody
	public String generateUserCode() {
	    log.info("┌───────────────────────────────────────┐");
	    log.info("│ **generateUserCode()**                │");
	    log.info("└───────────────────────────────────────┘");

	    // 랜덤한 userCode 생성 (예: 1000에서 9999 사이의 숫자)
	    int userCode = (int) (Math.random() * (9999 - 1000 + 1)) + 1000;

	    // JSON 응답
	    return new Gson().toJson(new MessageVO(1, String.valueOf(userCode)));
	}

	
	@GetMapping("/user_reg_index.do")
	public String userRegIndex() {
		String viewName = "user/user_reg";
		log.debug("┌───────────────────────────────────────┐");
		log.debug("│ **userRegIndex()**                    │");
		log.debug("└───────────────────────────────────────┘");		
		
		return "user/user_reg";
	}
	
	@GetMapping("/doRetrieve.do")
	public String doRetrieve(HttpServletRequest req, Model model) throws Exception {
	    String viewName = "user/user_list";
	    SearchBoardVO search = new SearchBoardVO();
	    log.debug("┌───────────────────────────────────────┐");
	    log.debug("│ **doRetrieve()**                      │");
	    log.debug("└───────────────────────────────────────┘");

	    String pageNoString = StringUtil.nvl(req.getParameter("pageNo"), "1");
	    String pageSizeString = StringUtil.nvl(req.getParameter("pageSize"), "10");

	    int pageNo = Integer.parseInt(pageNoString);
	    int pageSize = Integer.parseInt(pageSizeString);
	    log.debug("pageNo:{}", pageNo);
	    log.debug("pageSize:{}", pageSize);

	    // 데이터가 null이면 ""
	    String searchDiv = StringUtil.nvl(req.getParameter("searchDiv"), "");
	    String searchWord = StringUtil.nvl(req.getParameter("searchWord"), "");
	    log.debug("searchDiv:{}", searchDiv);
	    log.debug("searchWord:{}", searchWord);

	    search.setPageNo(pageNo);
	    search.setPageSize(pageSize);
	    search.setSearchDiv(searchDiv);
	    search.setSearchWord(searchWord);

	    log.debug("search:{}", search);

	    // 검색조건에 맞는 사용자 리스트 조회
	    List<UserVO> list = userService.doRetrieve(search);

	    // 총 글수
	    int totalCnt = 0;
	    if (null != list && list.size() > 0) {
	        UserVO vo = list.get(0);
	        totalCnt = vo.getTotalCnt();
	    }

	    model.addAttribute("totalCnt", totalCnt);
	    model.addAttribute("list", list);
	    model.addAttribute("search", search);

	    return viewName;
	}

	
	
	@GetMapping("/doSelectOne.do")
	public String doSelectOne(Model model,
            @RequestParam(name = "userId", defaultValue = "")String userId) {
			String viewName = "user/user_update";
			
			log.info("┌───────────────────────────────────────┐");
			log.info("│ **doSelectOne()**                     │");
			log.info("└───────────────────────────────────────┘");			
			
			log.info("paran userId:"+userId);
			
			if("".equals(userId)) {
				throw new DataNotFoundException("userId를 확인 하세요.");
			}
			
			UserVO inVO=new UserVO();
			inVO.setUserId(userId);
			
			// userId를 바탕으로 사용자의 정보를 가져옵니다.
			UserVO outVO = userService.doSelectOne(inVO);
			log.info("outVO:"+outVO);
			
			// Model에 사용자 정보를 담아 수정 페이지로 전달
			model.addAttribute("outVO", outVO);
			
			return viewName;
	}
	
	@PostMapping(value = "/doSave.do", produces = "text/plain;charset=UTF-8")
	@ResponseBody
	public String doSave(UserVO inVO) {
	    String jsonString = "";
	    log.info("┌───────────────────────────────────────┐");
	    log.info("│ **doSave()**                          │");
	    log.info("└───────────────────────────────────────┘");

	    log.info("paran:" + inVO);
	    
//	    if ("".equals(inVO.getUserId())) {
//            return "redirect:/user/user_reg"; // 오류 발생 시 회원 가입 페이지로 리다이렉션
//        }

	    if ("".equals(inVO.getUserId())) {
	        return new Gson().toJson(new MessageVO(20, "사용자 ID를 입력 하세요."));
	    }
	    
	    int nicknameCount = userService.checkNickname(inVO.getNickname());
        if (nicknameCount > 0) {
            return new Gson().toJson(new MessageVO(20, "이미 존재하는 닉네임입니다."));
        }

	    // 사용자 저장 처리
	    int flag = this.userService.doSave(inVO);

	    String message = "";
	    if (1 == flag) {
	        message = "사용자가 등록 되었습니다.";
	    } else {
	        message = "사용자 등록 실패 입니다.";
	        return "redirect:/user/user_reg"; // 등록 실패 시 회원 가입 페이지로 리다이렉션
	    }

	    MessageVO messageVO = new MessageVO(flag, message);

	    jsonString = new Gson().toJson(messageVO);
	    log.info("jsonString:" + jsonString);

	    return jsonString;
	}

	
 // 사용자 삭제 (userId로)
    @GetMapping(value = "/doDelete.do", produces = "text/plain;charset=UTF-8")
    @ResponseBody
    public String doDelete(@RequestParam(name = "userId", defaultValue = "") String userId) {
        String jsonString = "";
        log.info("┌───────────────────────────────────────┐");
        log.info("│ **doDelete()**                        │");
        log.info("└───────────────────────────────────────┘");

        log.info("userId:{}", userId);
        if ("".equals(userId)) {
            return new Gson().toJson(new MessageVO(20, "userId 데이터를 확인하세요."));
        }

        UserVO inVO = new UserVO();
        inVO.setUserId(userId);

        int flag = this.userService.doDelete(inVO);

        String message = "";
        if (1 == flag) {
            message = "사용자가 삭제 되었습니다.";
        } else {
            message = "사용자 삭제 실패 입니다.";
        }

        MessageVO messageVO = new MessageVO(flag, message);

        jsonString = new Gson().toJson(messageVO);
        log.info("jsonString:" + jsonString);

        return jsonString;
    }

    // 사용자 정보 수정 (userId로)
    @PostMapping(value = "/doUpdate.do", produces = "text/plain;charset=UTF-8")    
    public String doUpdate(UserVO inVO) {
        String jsonString = "";
        log.info("┌───────────────────────────────────────┐");
        log.info("│ **doUpdate()**                          │");
        log.info("└───────────────────────────────────────┘");

        if ("".equals(inVO.getName())) {
            return new Gson().toJson(new MessageVO(20, "사용자 이름을 입력 하세요."));
        }
        
        int nicknameCount = userService.checkNickname(inVO.getNickname());
        if (nicknameCount > 0) {
            return new Gson().toJson(new MessageVO(20, "이미 존재하는 닉네임입니다."));
        }

        int flag = userService.doUpdate(inVO);

        String message = "";
        if (1 == flag) {
            message = "사용자 정보가 수정 되었습니다.";
        } else {
            message = "사용자 정보 수정 실패 입니다.";
        }
        
        log.info("flag:" + flag);

        MessageVO messageVO = new MessageVO(flag, message);

        jsonString = new Gson().toJson(messageVO);
        log.info("jsonString:" + jsonString);

        return "redirect:/main_user/doRetrieve.do";
    }
    
 // 아이디 중복 체크
    @GetMapping("/checkUserId.do")
    @ResponseBody
    public String checkUserId(@RequestParam("userId") String userId) {
        log.info("┌───────────────────────────────────────┐");
        log.info("│           **checkUserId()**           │");
        log.info("└───────────────────────────────────────┘");

        int count = userService.checkUserId(userId);
        boolean isDuplicated = (count > 0);

        MessageVO messageVO = new MessageVO(isDuplicated ? 1 : 0, isDuplicated ? "아이디가 이미 존재합니다." : "사용 가능한 아이디입니다.");
        return new Gson().toJson(messageVO);
    }
    
    // 닉네임 중복 체크
    @GetMapping("/checkNickname.do")
    @ResponseBody
    public String checkNickname(@RequestParam("nickname") String nickname) {
        log.info("┌───────────────────────────────────────┐");
        log.info("│           **checkNickname()**           │");
        log.info("└───────────────────────────────────────┘");

        int count = userService.checkNickname(nickname);
        boolean isDuplicated = (count > 0);

        MessageVO messageVO = new MessageVO(isDuplicated ? 1 : 0, isDuplicated ? "닉네임이 이미 존재합니다." : "사용 가능한 닉네임입니다.");
        return new Gson().toJson(messageVO);
    }
    
    /**
     * 특정 사용자를 관리자로 지정하는 기능.
     * 기존 관리자가 있으면 해제한 후 지정.
     */
    @GetMapping("/setAdmin.do")
    public String setAdmin(@RequestParam("userId") String userId, Model model) throws Exception {
        log.info("관리자 지정 요청, userId: {}", userId);
        int flag = userService.setAdmin(userId);
        if(flag > 0) {
            model.addAttribute("message", "관리자 계정이 변경되었습니다.");
        } else {
            model.addAttribute("message", "관리자 계정 변경에 실패했습니다.");
        }
        // 사용자 목록 페이지로 리다이렉트
        return "redirect:/main_user/doRetrieve.do";
    }
	
}

