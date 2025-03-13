package com.pcwk.ehr.reply.mapper;

import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.reply.domain.ReplyVO;

@Mapper
public interface ReplyMapper {
	
	List<ReplyVO> getReply(BoardVO boardVO);
	
	int doSave(ReplyVO replyVO) throws SQLException;
	
	int doDelete(ReplyVO replyVO) throws SQLException;
	
	int doUpdate(ReplyVO replyVO) throws SQLException;
}
