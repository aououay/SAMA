package com.pcwk.ehr.user.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.pcwk.ehr.cmn.WorkDiv;
import com.pcwk.ehr.user.domain.UserVO;

@Mapper
public interface UserMapper extends WorkDiv<UserVO> {

	void selectUserById();
	
	void insertUser();
	
	UserVO doSearch(int userCode);
	
	int doCheckEmail(String email);
	
	int checkUserId(String userId);
	
	int checkNickname(String nickname);

	UserVO doSelectOneByEmail(String email);
	
	// 관리자 관련 메서드
    /**
     * 현재 관리자 계정 해제 (IS_ADMIN = 'Y' 인 모든 행을 'N'으로 업데이트)
     */
    int removeCurrentAdmin();

    /**
     * 특정 userId를 관리자 계정으로 지정 (IS_ADMIN을 'Y'로 업데이트)
     */
    int setAdmin(String userId);
}
