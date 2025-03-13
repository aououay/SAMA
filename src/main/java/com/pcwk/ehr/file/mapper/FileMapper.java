package com.pcwk.ehr.file.mapper;

import java.sql.SQLException;

import org.apache.ibatis.annotations.Mapper;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.file.domain.FileVO;

@Mapper
public interface FileMapper {

	int saveFile(FileVO fileVO) throws SQLException;
	
	FileVO getFile(BoardVO boardVO) throws SQLException, NullPointerException;
	
	int deleteFile(BoardVO boardVO) throws SQLException;
	
	FileVO getFileBySaveFileName(String saveFileName);
	
}
