package com.pcwk.ehr.board.mapper;

import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.cmn.BoardDTO;
import com.pcwk.ehr.cmn.DTO;
import com.pcwk.ehr.reply.domain.ReplyVO;

@Mapper
public interface BoardMapper {
	
	List<BoardVO> doRetrieve(BoardDTO dto);
	
	int doSave(BoardVO boardVO) throws SQLException;
	
	BoardVO doSelectOne(BoardVO boardVO) throws SQLException, NullPointerException;
	
	int doReadCntUpdate(BoardVO boardVO) throws SQLException;
	
	int doDelete(BoardVO boardVO) throws SQLException;
	
	int doUpdate(BoardVO boardVO) throws SQLException;
	
	int getBoardSequence();
	
	List<BoardVO> getLatestFiveBoard(@Param(value = "div") String div);
}
