package com.pcwk.ehr.login.service;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.login.mapper.LoginMapper;
import com.pcwk.ehr.user.domain.UserVO;
import com.pcwk.ehr.user.mapper.UserMapper;

@Service
public class LoginServiceImpl implements LoginService {
	
	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	LoginMapper loginMapper;
	
	public LoginServiceImpl() {
		super();
	}

	@Override
	public int idPassCheck(UserVO inVO) throws SQLException {
		int flag = 30;
		//flag = 10 : id없음
		//flag = 20 : 비번 불일치 
		//flag = 30 : id, 비번 일치
		
		
		int count = loginMapper.idCheck(inVO);
		if(1 != count) {
			flag = 10;
			return flag;
		}
		
		count = loginMapper.idPassCheck(inVO);
		if(1 != count) {
			flag = 20;
			return flag;			
		}
		
		log.debug("flag: {}",flag);
		
		return flag;
	}

	@Override
	public UserVO doSelectOne(UserVO inVO) throws Exception {
		return loginMapper.doSelectOne(inVO);
	}
	
	
	
	

}
