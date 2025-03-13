package com.pcwk.ehr.email.service;

import java.sql.SQLException;
import java.util.Random;

import org.springframework.dao.DataAccessException;

import com.pcwk.ehr.email.domain.EmailVO;

public interface EmailService {

    /**
     * 이메일 등록
     * @param email
     * @return 0(실패)/1(성공)
     * @throws SQLException
     */
    int doSave(String email) throws SQLException;

    /**
     * 이메일 중복 확인
     * @param inVO
     * @return 1(중복O)/ 0(중복x)
     * @throws SQLException
     */
    int isExistsEmail(EmailVO inVO) throws SQLException, DataAccessException;

    /**
     * 인증코드 업데이트
     * @param inVO
     * @return 0(실패)/1(성공)
     * @throws SQLException
     */
    int doUpdate_Auth_Code(EmailVO inVO) throws SQLException;

    /**
     * 회원가입여부_1 업데이트
     * @param email
     * @return 0(실패)/1(성공)
     * @throws SQLException
     */
    int doUpdate_reg_stat_1(String email) throws SQLException;

    /**
     * 회원가입여부_0 업데이트
     * @param email
     * @return 0(실패)/1(성공)
     * @throws SQLException
     */
    int doUpdate_reg_stat_0(String email) throws SQLException;

    /**
     * 이메일 단건 조회
     * @param inVO
     * @return EmailVO 객체
     * @throws SQLException
     * @throws NullPointerException
     */
    EmailVO doSelectOne(EmailVO inVO) throws SQLException, NullPointerException;
    
    /**
     * 비밀번호 변경
     * @param email 사용자 이메일
     * @param newPassword 새로운 비밀번호
     * @return 0(실패)/1(성공)
     * @throws SQLException
     */
    int changePassword(String email, String newPassword) throws SQLException;

    void send_Auth_Code(EmailVO inVO) throws Exception;

    static String AuthCodeGenerator() {
        Random random = new Random();
        int randomNumber = random.nextInt(900000) + 100000;
        return String.valueOf(randomNumber);
    }

    // New method for verifying the auth code
    int verifyAuthCode(EmailVO inVO) throws Exception;
}
