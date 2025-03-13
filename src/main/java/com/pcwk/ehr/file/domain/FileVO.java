package com.pcwk.ehr.file.domain;

import com.pcwk.ehr.cmn.BoardDTO;

public class FileVO extends BoardDTO {
	
	private String orgFileName; //원본 파일명
	private String saveFileName;//저장 파일명(동적으로 생성)
	private String savePath;    //저장경로
	private long   fileSize;    //파일크기
	private String extension;   //확장자
	private int boardId;
	
	public FileVO() {}

	public FileVO(String orgFileName, String saveFileName, String savePath, long fileSize, String extension,
			int boardId) {
		super();
		this.orgFileName = orgFileName;
		this.saveFileName = saveFileName;
		this.savePath = savePath;
		this.fileSize = fileSize;
		this.extension = extension;
		this.boardId = boardId;
	}

	public String getOrgFileName() {
		return orgFileName;
	}

	public void setOrgFileName(String orgFileName) {
		this.orgFileName = orgFileName;
	}

	public String getSaveFileName() {
		return saveFileName;
	}

	public void setSaveFileName(String saveFileName) {
		this.saveFileName = saveFileName;
	}

	public String getSavePath() {
		return savePath;
	}

	public void setSavePath(String savePath) {
		this.savePath = savePath;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	public int getBoardId() {
		return boardId;
	}

	public void setBoardId(int boardId) {
		this.boardId = boardId;
	}

	@Override
	public String toString() {
		return "FileVO [orgFileName=" + orgFileName + ", saveFileName=" + saveFileName + ", savePath=" + savePath
				+ ", fileSize=" + fileSize + ", extension=" + extension + ", boardId=" + boardId + "]";
	}
	
	
	
}
