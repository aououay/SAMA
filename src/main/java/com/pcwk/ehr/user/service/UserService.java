package com.pcwk.ehr.user.service;

import com.pcwk.ehr.cmn.WorkDiv;
import com.pcwk.ehr.user.domain.UserVO;

public interface UserService extends WorkDiv<UserVO> {
	
	UserVO doSearch(int userCode);
	
	int doCheckEmail(String email);
	
    int checkUserId(String userId); 

    int checkNickname(String nickname);

    UserVO findUserByEmail(String email) throws Exception;
    
    /**
     * 지정된 userId를 관리자로 지정합니다.
     * 기존 관리자가 있다면 먼저 관리자 권한을 해제한 후 새 계정에 관리자 권한을 부여합니다.
     * @param userId 새 관리자 계정으로 지정할 사용자 ID
     * @return 업데이트된 행의 수
     */
    int setAdmin(String userId) throws Exception;
}
