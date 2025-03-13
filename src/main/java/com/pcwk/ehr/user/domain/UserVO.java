package com.pcwk.ehr.user.domain;

import com.pcwk.ehr.cmn.BoardDTO;

public class UserVO extends BoardDTO {

	private String email;
	private String userId;
	private String password;
	private String newPassword;
	private String regDate;
	private String modDate;
	private String name;
	private String nickname;
	private String address;
	private String phone;
	private String isAdmin;

	public UserVO() {
		super();
	}

	public UserVO(String email, String userId, String password, String newPassword, String regDate, String modDate,
			String name, String nickname, String address, String phone, String isAdmin) {
		super();
		this.email = email;
		this.userId = userId;
		this.password = password;
		this.newPassword = newPassword;
		this.regDate = regDate;
		this.modDate = modDate;
		this.name = name;
		this.nickname = nickname;
		this.address = address;
		this.phone = phone;
		this.isAdmin = isAdmin;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getNewPassword() {
		return newPassword;
	}

	public void setNewPassword(String newPassword) {
		this.newPassword = newPassword;
	}

	public String getRegDate() {
		return regDate;
	}

	public void setRegDate(String regDate) {
		this.regDate = regDate;
	}

	public String getModDate() {
		return modDate;
	}

	public void setModDate(String modDate) {
		this.modDate = modDate;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getNickname() {
		return nickname;
	}

	public void setNickname(String nickname) {
		this.nickname = nickname;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getIsAdmin() {
		return isAdmin;
	}

	public void setIsAdmin(String isAdmin) {
		this.isAdmin = isAdmin;
	}

	@Override
	public String toString() {
		return "UserVO [email=" + email + ", userId=" + userId + ", password=" + password + ", newPassword="
				+ newPassword + ", regDate=" + regDate + ", modDate=" + modDate + ", name=" + name + ", nickname="
				+ nickname + ", address=" + address + ", phone=" + phone + ", isAdmin=" + isAdmin + "]";
	}
}
