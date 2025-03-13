package com.pcwk.ehr.board.domain;

import com.pcwk.ehr.cmn.BoardDTO;

public class BoardVO extends BoardDTO {
	private int boardId;
	private String title;
	private String content;
	private int readCnt;
	private String regId;
	private String regDt;
	private String modDt;
	private String div;
	
	public BoardVO() {
		super();
		// TODO Auto-generated constructor stub
	}

	public BoardVO(int boardId, String title, String content, int readCnt, String regId, String regDt, String modDt,
			String div) {
		super();
		this.boardId = boardId;
		this.title = title;
		this.content = content;
		this.readCnt = readCnt;
		this.regId = regId;
		this.regDt = regDt;
		this.modDt = modDt;
		this.div = div;
	}

	public int getBoardId() {
		return boardId;
	}

	public void setBoardId(int boardId) {
		this.boardId = boardId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public int getReadCnt() {
		return readCnt;
	}

	public void setReadCnt(int readCnt) {
		this.readCnt = readCnt;
	}

	public String getRegId() {
		return regId;
	}

	public void setRegId(String regId) {
		this.regId = regId;
	}

	public String getRegDt() {
		return regDt;
	}

	public void setRegDt(String regDt) {
		this.regDt = regDt;
	}

	public String getModDt() {
		return modDt;
	}

	public void setModDt(String modDt) {
		this.modDt = modDt;
	}

	public String getDiv() {
		return div;
	}

	public void setDiv(String div) {
		this.div = div;
	}

	@Override
	public String toString() {
		return "BoardVO [boardId=" + boardId + ", title=" + title + ", content=" + content + ", readCnt=" + readCnt
				+ ", regId=" + regId + ", regDt=" + regDt + ", modDt=" + modDt + ", div=" + div + "]";
	}
	
	
}
