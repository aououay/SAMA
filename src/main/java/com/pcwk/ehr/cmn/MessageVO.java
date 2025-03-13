package com.pcwk.ehr.cmn;

public class MessageVO extends DTO {
	private int messageId; //상태:1(성공)/1이외는 실패
	private String message;//메시지
	
	public MessageVO() {
		super();

	}

	public MessageVO(int messageId, String message) {
		super();
		this.messageId = messageId;
		this.message = message;
	}

	public int getMessageId() {
		return messageId;
	}

	public void setMessageId(int messageId) {
		this.messageId = messageId;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "MessageVO [messageId=" + messageId + ", message=" + message + "]";
	}
	
	
}
