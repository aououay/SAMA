package com.pcwk.ehr.login.service;

import java.sql.SQLException;

import org.springframework.stereotype.Service;

import com.pcwk.ehr.user.domain.UserVO;

@Service
public interface LoginService {
	
	/**
	 * 단건 조회
	 * @param inVO
	 * @return UserVO
	 * @throws Exception
	 */
	UserVO doSelectOne(UserVO inVO) throws Exception;
	

	/**
	 * id,비번check
	 * @param inVO
	 * @return int
	 * @throws SQLException
	 */
	int idPassCheck(UserVO inVO) throws SQLException;
	

}




