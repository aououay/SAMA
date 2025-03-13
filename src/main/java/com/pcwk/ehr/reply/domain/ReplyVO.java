package com.pcwk.ehr.reply.domain;

public class ReplyVO {

	private int replyId;
	private int boardId;
	private String regId;
	private String nickname;
	private String replyContent;
	private String regDt;
	private String modDt;
	
	public ReplyVO() {
		super();
	}
	
	public ReplyVO(int replyId, int boardId, String regId, String nickname, String replyContent, String regDt,
			String modDt) {
		super();
		this.replyId = replyId;
		this.boardId = boardId;
		this.regId = regId;
		this.nickname = nickname;
		this.replyContent = replyContent;
		this.regDt = regDt;
		this.modDt = modDt;
	}

	public int getReplyId() {
		return replyId;
	}

	public void setReplyId(int replyId) {
		this.replyId = replyId;
	}

	public int getBoardId() {
		return boardId;
	}

	public void setBoardId(int boardId) {
		this.boardId = boardId;
	}

	public String getRegId() {
		return regId;
	}

	public void setRegId(String regId) {
		this.regId = regId;
	}

	public String getNickname() {
		return nickname;
	}

	public void setNickname(String nickname) {
		this.nickname = nickname;
	}

	public String getReplyContent() {
		return replyContent;
	}

	public void setReplyContent(String replyContent) {
		this.replyContent = replyContent;
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

	@Override
	public String toString() {
		return "ReplyVO [replyId=" + replyId + ", boardId=" + boardId + ", regId=" + regId + ", nickname=" + nickname
				+ ", replyContent=" + replyContent + ", regDt=" + regDt + ", modDt=" + modDt + "]";
	}
	
	
	
	
	
}
