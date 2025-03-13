package com.pcwk.ehr.file.service;

import java.sql.SQLException;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.file.domain.FileVO;

public interface FileService {
	
	int saveFile(FileVO fileVO) throws SQLException;
	
	FileVO getFile(BoardVO boardVO) throws SQLException, NullPointerException;
	
	int deleteFile(BoardVO boardVO) throws SQLException;
	
	FileVO getFileBySaveFileName(String saveFileName);
}
