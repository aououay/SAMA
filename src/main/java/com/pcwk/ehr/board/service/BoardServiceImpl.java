package com.pcwk.ehr.board.service;

import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.board.mapper.BoardMapper;
import com.pcwk.ehr.cmn.BoardDTO;
import com.pcwk.ehr.cmn.DTO;
import com.pcwk.ehr.file.domain.FileVO;
import com.pcwk.ehr.reply.domain.ReplyVO;

@Service
public class BoardServiceImpl implements BoardService {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	BoardMapper boardMapper;

	public BoardServiceImpl() {
		super();
		log.info("┌──────────────────────────┐");
		log.info("│ BoardServiceImpl()       │");
		log.info("└──────────────────────────┘");
	}

	@Override
	public List<BoardVO> doRetrieve(BoardDTO dto) {

		return boardMapper.doRetrieve(dto);
	}

	@Override
	public int doSave(BoardVO boardVO) throws SQLException {

		return boardMapper.doSave(boardVO);
	}

	@Override
	public BoardVO doSelectOne(BoardVO boardVO) throws SQLException, NullPointerException {
		log.info("┌──────────────────────────┐");
		log.info("│ doReadCntUpdate()        │");
		log.info("└──────────────────────────┘");

		int flag = boardMapper.doReadCntUpdate(boardVO);
		log.info("flag:{}", flag);

		return boardMapper.doSelectOne(boardVO);
	}

	@Override
	public int doDelete(BoardVO boardVO) throws SQLException {

		return boardMapper.doDelete(boardVO);
	}

	@Override
	public int doUpdate(BoardVO boardVO) throws SQLException {

		return boardMapper.doUpdate(boardVO);
	}

	@Override
	public int getBoardSequence() {

		return boardMapper.getBoardSequence();
	}

	@Override
	public List<BoardVO> getLatestFiveBoard(String div) {
		return boardMapper.getLatestFiveBoard(div);
	}

}
