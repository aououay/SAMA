package com.pcwk.ehr.email.domain;

import com.pcwk.ehr.cmn.BoardDTO;

public class EmailVO extends BoardDTO {

	private String email; // 이메일
	private String auth_code; // 인증번호
	private int reg_stat; // 회원가입여부

	public EmailVO() {
		super();
	}

	public EmailVO(String email, String auth_code, int reg_stat) {
		super();
		this.email = email;
		this.auth_code = auth_code;
		this.reg_stat = reg_stat;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getAuth_code() {
		return auth_code;
	}

	public void setAuth_code(String auth_code) {
		this.auth_code = auth_code;
	}

	public int getReg_stat() {
		return reg_stat;
	}

	public void setReg_stat(int reg_stat) {
		this.reg_stat = reg_stat;
	}

	@Override
	public String toString() {
		return "EmailVO [email=" + email + ", auth_code=" + auth_code + ", reg_stat=" + reg_stat + ", getEmail()="
				+ getEmail() + ", getAuth_code()=" + getAuth_code() + ", getReg_stat()=" + getReg_stat()
				+ ", toString()=" + super.toString() + "]";
	}

}