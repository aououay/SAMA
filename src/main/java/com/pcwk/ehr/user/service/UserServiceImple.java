package com.pcwk.ehr.user.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.cmn.BoardDTO;
import com.pcwk.ehr.cmn.DTO;
import com.pcwk.ehr.user.domain.UserVO;
import com.pcwk.ehr.user.mapper.UserMapper;

@Service
public class UserServiceImple implements UserService {
	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	UserMapper mapper;

	public UserServiceImple() {
		super();
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **UserServiceImple()**                │");
		log.info("└───────────────────────────────────────┘");
	}

	@Override
	public int doDelete(UserVO inVO) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **doDelete()**                        │");
		log.info("└───────────────────────────────────────┘");
		return mapper.doDelete(inVO);
	}

	@Override
	public int doUpdate(UserVO inVO) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **doUpdate()**                        │");
		log.info("└───────────────────────────────────────┘");
		return mapper.doUpdate(inVO);
	}

	@Override
	public List<UserVO> doRetrieve(BoardDTO dto) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **doRetrieve()**                      │");
		log.info("└───────────────────────────────────────┘");
		return mapper.doRetrieve(dto);
	}

	@Override
	public int doSave(UserVO inVO) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **doSave()**                          │");
		log.info("└───────────────────────────────────────┘");
		return mapper.doSave(inVO);
	}

	@Override
	public UserVO doSelectOne(UserVO inVO) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **doSelectOne()**                     │");
		log.info("└───────────────────────────────────────┘");
		return mapper.doSelectOne(inVO);
	}

	@Override
	public UserVO doSearch(int userCode) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **doSearch()**                        │");
		log.info("└───────────────────────────────────────┘");
		return mapper.doSearch(userCode);
	}

	@Override
	public int doCheckEmail(String email) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **doCheckEmail()**                    │");
		log.info("└───────────────────────────────────────┘");
		return mapper.doCheckEmail(email);
	}

	@Override
	public int checkUserId(String userId) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **checkUserId()**                     │");
		log.info("└───────────────────────────────────────┘");
		return mapper.checkUserId(userId);
	}

	@Override
	public UserVO findUserByEmail(String email) throws Exception {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **findUserByEmail()**                     │");
		log.info("└───────────────────────────────────────┘");
		return mapper.doSelectOneByEmail(email);
	}

	@Override
	public int setAdmin(String userId) throws Exception {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **setAdmin()**                        │");
		log.info("└───────────────────────────────────────┘");
		// 기존 관리자를 해제
		int removeCount = mapper.removeCurrentAdmin();
		log.info("removeCount: {}", removeCount);
		// 새 관리자 지정
		int setCount = mapper.setAdmin(userId);
		log.info("setCount: {}", setCount);
		return setCount;
	}

	@Override
	public int checkNickname(String nickname) {
		log.info("┌───────────────────────────────────────┐");
		log.info("│ **checkNickname()**                     │");
		log.info("└───────────────────────────────────────┘");
		return mapper.checkNickname(nickname);
	}

	@Override
	public List<UserVO> doRetrieve(DTO dto) {
		// TODO Auto-generated method stub
		return null;
	}

}
