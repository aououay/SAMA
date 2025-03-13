package com.pcwk.ehr.board.service;

import java.sql.SQLException;
import java.util.List;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.cmn.BoardDTO;

public interface BoardService {
		
	List<BoardVO> doRetrieve(BoardDTO dto);
	
	int doSave(BoardVO boardVO) throws SQLException;
	
	BoardVO doSelectOne(BoardVO boardVO) throws SQLException, NullPointerException;
	
	int doDelete(BoardVO boardVO) throws SQLException;
	
	int doUpdate(BoardVO boardVO) throws SQLException;
	
	int getBoardSequence();
	
	List<BoardVO> getLatestFiveBoard(String div);
	
	
	
	
	}
