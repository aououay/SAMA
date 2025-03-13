package com.pcwk.ehr.login.mapper;

import java.sql.SQLException;

import org.apache.ibatis.annotations.Mapper;

import com.pcwk.ehr.cmn.WorkDiv;
import com.pcwk.ehr.user.domain.UserVO;

@Mapper
public interface LoginMapper extends WorkDiv<UserVO> {
	
	int idCheck(UserVO inVo) throws SQLException;
	
	int idPassCheck(UserVO inVo) throws SQLException;
	
	
	
}
