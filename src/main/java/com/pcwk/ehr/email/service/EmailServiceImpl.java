package com.pcwk.ehr.email.service;

import java.sql.SQLException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.apache.ibatis.annotations.Mapper;

import com.pcwk.ehr.email.domain.EmailVO;
import com.pcwk.ehr.email.mapper.EmailMapper;

@Service
@Transactional
public class EmailServiceImpl implements EmailService {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private EmailMapper emailMapper;

    @Autowired
    private JavaMailSender mailSender;

    @Override
    public int doSave(String email) throws SQLException {
        return emailMapper.doSave(email);
    }

    @Override
    public int isExistsEmail(EmailVO inVO) throws SQLException, DataAccessException {
        return emailMapper.isExistsEmail(inVO);
    }

    @Override
    public int doUpdate_Auth_Code(EmailVO inVO) throws SQLException {
    	
    	log.info("doUpdate_Auth_Code:"+inVO);
        return emailMapper.doUpdate_Auth_Code(inVO);
    }

    @Override
    public int doUpdate_reg_stat_1(String email) throws SQLException {
        return emailMapper.doUpdate_reg_stat_1(email);
    }

    @Override
    public int doUpdate_reg_stat_0(String email) throws SQLException {
        return emailMapper.doUpdate_reg_stat_0(email);
    }

    @Override
    public EmailVO doSelectOne(EmailVO inVO) throws SQLException, NullPointerException {
        return emailMapper.doSelectOne(inVO);
    }

    @Override
    public void send_Auth_Code(EmailVO inVO) throws Exception {
        try {
            SimpleMailMessage message = new SimpleMailMessage();

            String auth_Code = EmailService.AuthCodeGenerator();

            log.info("인증 번호 : {}", auth_Code);
            // 보내는 사람
            message.setFrom("e_dunkel@naver.com");

            // 받는 사람
            message.setTo(inVO.getEmail());

            // 제목
            message.setSubject("서울 365 재난안전포털 인증코드  : " + auth_Code);

            // 내용:
            message.setText("이용자님, 안녕하세요. \n" + "현재 본인 인증하기 중입니다. \n" + "귀하의 인증코드는  "
                    + auth_Code + "  입니다.\n" + "서울 365 재난안전포털\n" +
                    "※본 메일은 자동응답 메일이므로 본 메일에 회신하지 마시기 바랍니다. ");

            mailSender.send(message);

            inVO.setAuth_code(auth_Code);
            emailMapper.doUpdate_Auth_Code(inVO);
            log.info("메일 전송 완료: {}", inVO.getEmail());
        } catch (MailException e) {
            log.info("메일 전송 실패: {}", e.getMessage(), e);
            throw new Exception("메일 전송에 실패했습니다.", e);
        } catch (SQLException e) {
            log.info("인증 코드 업데이트 실패: {}", e.getMessage(), e);
            throw new Exception("인증 코드 업데이트에 실패했습니다.", e);
        }
    }
    
    @Override
    public int changePassword(String email, String newPassword) throws SQLException {
        try {
            // 비밀번호 변경 메서드 호출
            return emailMapper.changePassword(email, newPassword);
        } catch (SQLException e) {
            log.error("비밀번호 변경 중 오류 발생", e);
            throw new SQLException("비밀번호 변경 중 오류 발생", e);
        }
    }

    @Override
    public int verifyAuthCode(EmailVO inVO) throws Exception {
        return emailMapper.verifyAuthCode(inVO);
    }
}
