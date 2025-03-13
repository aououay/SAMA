package com.pcwk.ehr.reply.controller;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;
import com.pcwk.ehr.board.service.BoardService;
import com.pcwk.ehr.cmn.MessageVO;
import com.pcwk.ehr.reply.domain.ReplyVO;
import com.pcwk.ehr.reply.service.ReplyService;
import com.pcwk.ehr.user.domain.UserVO;

import jakarta.servlet.http.HttpSession;

@RequestMapping("reply")
@Controller
public class ReplyController {

	final Logger log = LoggerFactory.getLogger(getClass());
	
	@Autowired
	BoardService boardService;
	
	@Autowired
	ReplyService replyService;

	public ReplyController() {
		log.info("┌──────────────────────────┐");
		log.info("│ ReplyController()        │");
		log.info("└──────────────────────────┘");
	}
	
	@PostMapping(value = "/doSave.do", produces = "text/plain;charset=UTF-8")
	@ResponseBody
	public String doSave(ReplyVO replyVO, HttpSession session) throws SQLException {
	    String jsonString = "";
	    log.info("┌──────────────────────────┐");
	    log.info("│ doSave()                 │");
	    log.info("└──────────────────────────┘");			
	    log.info("replyVO before: {}", replyVO);
	    
	    // 로그인된 사용자의 닉네임을 세팅
	    // 여기서 sessionScope에 저장된 사용자 객체의 타입(UserVO)과 
	    // 닉네임 프로퍼티(getNickname())가 존재하는지 확인하세요.
	    UserVO user = (UserVO) session.getAttribute("user");
	    if(user != null) {
	        replyVO.setNickname(user.getNickname());
	    } else {
	        // 로그인되지 않았다면 기본값(빈 문자열) 또는 에러 처리
	        replyVO.setNickname("");
	    }
	    
	    log.info("replyVO after: {}", replyVO);
	    
	    int flag = this.replyService.doSave(replyVO);
	    log.info("flag: {}", flag);
	    
	    String message = (flag == 1) ? "댓글이 등록되었습니다." : "댓글 등록 실패.";
	    
	    MessageVO messageVO = new MessageVO(flag, message);
	    
	    jsonString = new Gson().toJson(messageVO);
	    log.info("jsonString:\n{}", jsonString);  
	    
	    return jsonString;
	}

	
	
	@GetMapping(value = "/doDelete.do", produces = "text/plain;charset=UTF-8" )
	@ResponseBody
	public String doDelete(@RequestParam(name="replyId", required = true, defaultValue = "0")int replyId) throws SQLException {
		String jsonString = "";
		
		log.info("┌──────────────────────────┐");
		log.info("│ doDelete()               │");
		log.info("└──────────────────────────┘");				
		
		ReplyVO replyVO = new ReplyVO();
		replyVO.setReplyId(replyId);
		log.info("replyId:{}",replyId);
		
		int flag = this.replyService.doDelete(replyVO);
		log.info("flag:{}",flag);
		
		String message = "";
		if(1 == flag) {
			message = "댓글이 삭제 되었습니다.";
		}else {
			message = "댓글 삭제 실패.";
		}
		
		jsonString=new Gson().toJson(new MessageVO(flag, message));
		log.info("jsonString:\n{}",jsonString);
		
		return jsonString;
	}
	
	
	@PostMapping(value="/doUpdate.do", produces = "text/plain;charset=UTF-8" )
	@ResponseBody
	public String doUpdate(ReplyVO replyVO, HttpSession session)throws SQLException{
		String jsonString = "";
		log.info("┌──────────────────────────┐");
		log.info("│ doUpdate()               │");
		log.info("└──────────────────────────┘");
		
//		if(null != session.getAttribute("user")) {
//			UserVO user =(UserVO) session.getAttribute("user");
//			param.setModId(user.getUserId());
//			
//		}
		  
		log.info("replyVO:{}",replyVO);
		
		int flag = replyService.doUpdate(replyVO);
		String message = "";
		if( 1== flag) {
			message = "게시글이 수정 되었습니다.";
		}else {
			message = "게시글이 등록 되었습니다.";
		}
		
		MessageVO messageVO=new MessageVO(flag, message);
		
		jsonString = new Gson().toJson(messageVO);
		log.info("jsonString:\n{}",jsonString);  
		
		return jsonString;
	}
	

}
