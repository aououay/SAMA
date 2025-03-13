package com.pcwk.ehr.email.mapper;

import com.pcwk.ehr.email.domain.EmailVO;
import java.sql.SQLException;
import org.springframework.dao.DataAccessException;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface EmailMapper {
	
	// 이메일 저장
    int doSave(String email) throws SQLException;
    
    // 이메일 중복 확인
    int isExistsEmail(EmailVO inVO) throws SQLException, DataAccessException;
    
    // 인증코드 업데이트
    int doUpdate_Auth_Code(EmailVO inVO) throws SQLException;
    
    // 이메일 조회
    EmailVO doSelectOne(EmailVO inVO) throws SQLException;
    
    // 회원가입 상태 1로 업데이트
    int doUpdate_reg_stat_1(String email) throws SQLException;
    
    // 회원가입 상태 0으로 업데이트
    int doUpdate_reg_stat_0(String email) throws SQLException;
    
    // 인증 코드 검증
    int verifyAuthCode(EmailVO inVO) throws SQLException;
    
    // 비밀번호 변경
    int changePassword(@Param("email") String email, @Param("newPassword") String newPassword) throws SQLException;
}
