package com.pcwk.ehr.reply.service;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.board.mapper.BoardMapper;
import com.pcwk.ehr.reply.domain.ReplyVO;
import com.pcwk.ehr.reply.mapper.ReplyMapper;

@Service
public class ReplyServiceImpl implements ReplyService {

	final Logger log = LoggerFactory.getLogger(getClass());
	
	@Autowired
	ReplyMapper replyMapper;
	
	public ReplyServiceImpl() {
		super();
		log.info("┌──────────────────────────┐");
		log.info("│ BoardServiceImpl()       │");
		log.info("└──────────────────────────┘");
	}


	@Override
	public List<ReplyVO> getReply(BoardVO boardVO) {

		return replyMapper.getReply(boardVO);
	}


	@Override
	public int doSave(ReplyVO replyVO) throws SQLException {

		return replyMapper.doSave(replyVO);
	}


	@Override
	public int doDelete(ReplyVO replyVO) throws SQLException {

		return replyMapper.doDelete(replyVO);
	}


	@Override
	public int doUpdate(ReplyVO replyVO) throws SQLException {

		return replyMapper.doUpdate(replyVO);
	}



}
