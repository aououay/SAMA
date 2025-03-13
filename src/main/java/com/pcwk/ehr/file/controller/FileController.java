package com.pcwk.ehr.file.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;
import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.cmn.MessageVO;
import com.pcwk.ehr.file.domain.FileVO;
import com.pcwk.ehr.file.service.FileService;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;

@Controller
@RequestMapping("file")
public class FileController {

	final Logger log = LogManager.getLogger(getClass());

	@Autowired
	FileService fileService;
	
	//image파일
	final String IMG_PATH = "D:\\JAP_20240311_1\\04_SPRING\\boot\\workspace\\.metadata\\.plugins\\org.eclipse.wst.server.core\\tmp0\\wtpwebapps\\safety_master\\resources\\upload";
	
	//none image파일
	final String FILE_PATH = "D:\\upload";
	
	private String yyyyMMPath = "";// \\2025\\03
	private String fileFullPath  = "";// FILE_PATH+yyyyMMPath
	private String imageFullPath = "";// IMG_PATH+yyyyMMPath
	
	public FileController() {
		super();
		log.debug("┌───────────────────────────────────────┐");
		log.debug("│ *FileController()*                    │");
		log.debug("└───────────────────────────────────────┘");				
//		//이미지,일반파일 디렉토리 동적 생성!
//		
//		//년, 월 생성
//		String yyyy = StringUtil.getCurrentDate("yyyy");
//		String mm = StringUtil.getCurrentDate("MM");
//		log.debug("yyyy:{}",yyyy);
//		log.debug("mm:{}",mm);
//		
//		//디렉토리 동적 생성
//		yyyyMMPath = File.separator+yyyy+File.separator+mm;//  \2025\03
//		log.debug("yyyyMMPath:{}",yyyyMMPath);
//		log.debug("FILE_PATH+yyyyMMPath:{}",FILE_PATH+yyyyMMPath);		
//		
//		
//		//none image파일
//		fileFullPath = FILE_PATH+yyyyMMPath;
//		File normalFile=new File(fileFullPath);
//		
//		if(normalFile.isDirectory() == false) {
//			boolean isMakeDirs = normalFile.mkdirs();
//			log.debug("isMakeDirs:{}",isMakeDirs);
//		}
//		
//		log.debug("IMG_PATH+yyyyMMPath:{}",IMG_PATH+yyyyMMPath);
//		
//		imageFullPath = IMG_PATH+yyyyMMPath;
//		//image파일
//		File imageFile=new File(imageFullPath);
//		if(imageFile.isDirectory() == false) {
//			boolean isMakeDirs = imageFile.mkdirs();
//			log.debug("isMakeDirs:{}",isMakeDirs);			
//		}		
		
	}
	   
	@GetMapping("/downloadFile.do")
	public void downloadFile(@RequestParam("fileName") String saveFileName, HttpServletResponse response) throws IOException {
	    String fileDirectory = "D:/upload"; // 파일이 저장된 경로
	    
	    // 파일 정보 조회 (FileVO 객체에서 orgFileName 가져오기)
	    FileVO fileVO = fileService.getFileBySaveFileName(saveFileName); 
	    
	    if (fileVO == null) {
	        // 파일 정보가 없으면 오류 메시지 반환
	        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
	        response.getWriter().write("File not found.");
	        return;
	    }

	    String orgFileName = fileVO.getOrgFileName(); // 원본 파일명

	    File file = new File(fileDirectory, saveFileName); // 실제 파일 경로

	    if (file.exists()) {
	        // 파일의 MIME 타입을 설정합니다. (예: PDF, 이미지 등)
	        String mimeType = "application/octet-stream";
	        response.setContentType(mimeType);
	        response.setContentLengthLong(file.length());
	        
	        // 다운로드 시 원본 파일명으로 저장되도록 헤더 설정
	        response.setHeader("Content-Disposition", "attachment; filename=\"" + orgFileName + "\"");

	        try (FileInputStream fileInputStream = new FileInputStream(file);
	             ServletOutputStream outputStream = response.getOutputStream()) {

	            byte[] buffer = new byte[1024];
	            int bytesRead;
	            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
	                outputStream.write(buffer, 0, bytesRead);
	            }
	        }
	    } else {
	        // 파일이 존재하지 않는 경우
	        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
	        response.getWriter().write("File not found.");
	    }
	}

	
	@GetMapping(value = "/deleteFile.do", produces = "text/plain;charset=UTF-8" )
	@ResponseBody
	public String doDelete(@RequestParam(name="boardId", required = true, defaultValue = "0")int boardId) throws SQLException {
		String jsonString = "";
		
		log.info("┌──────────────────────────┐");
		log.info("│ deleteFile()             │");
		log.info("└──────────────────────────┘");				
		
		BoardVO boardVO = new BoardVO();
		boardVO.setBoardId(boardId);
		log.info("boardId:{}",boardId);
		
		int flag = this.fileService.deleteFile(boardVO);
		log.info("flag:{}",flag);
		
		String message = "";
		if(1 == flag) {
			message = "첨부파일이 삭제 되었습니다.";
		}else {
			message = "첨부파일 삭제 실패.";
		}
		
		jsonString=new Gson().toJson(new MessageVO(flag, message));
		log.info("jsonString:\n{}",jsonString);
		
		return jsonString;
	}
	
    

}
