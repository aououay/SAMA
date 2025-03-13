package com.pcwk.ehr.file.service;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.file.domain.FileVO;
import com.pcwk.ehr.file.mapper.FileMapper;

@Service
public class FileServiceImpl implements FileService {

	final Logger log = LoggerFactory.getLogger(getClass());
	
	@Autowired
	FileMapper fileMapper;
	
	public FileServiceImpl() {
		super();
		log.info("┌──────────────────────────┐");
		log.info("│ FileServiceImpl()        │");
		log.info("└──────────────────────────┘");
	}

	@Override
	public int saveFile(FileVO fileVO) throws SQLException {
	
		return fileMapper.saveFile(fileVO);
	}

	@Override
	public FileVO getFile(BoardVO boardVO) throws SQLException, NullPointerException {

		return fileMapper.getFile(boardVO);
	}

	@Override
	public int deleteFile(BoardVO boardVO) throws SQLException {
		
		return fileMapper.deleteFile(boardVO);
	}

	@Override
	public FileVO getFileBySaveFileName(String saveFileName) {
		// TODO Auto-generated method stub
		return fileMapper.getFileBySaveFileName(saveFileName);
	}

	

}
