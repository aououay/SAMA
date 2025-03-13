package com.pcwk.ehr.reply.service;

import java.sql.SQLException;
import java.util.List;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.reply.domain.ReplyVO;

public interface ReplyService {
		
	List<ReplyVO> getReply(BoardVO boardVO);
	
	int doSave(ReplyVO replyVO) throws SQLException;
	
	int doDelete(ReplyVO replyVO) throws SQLException;
	
	int doUpdate(ReplyVO replyVO) throws SQLException;
}
