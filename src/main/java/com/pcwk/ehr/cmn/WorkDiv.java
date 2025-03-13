package com.pcwk.ehr.cmn;

import java.util.List;

public interface WorkDiv<T> {
	/**
	 * 단건 삭제
	 * 
	 * @param inVO
	 * @return 1(성공)/0(실패)
	 */
	int doDelete(T inVO);

	/**
	 * 수정
	 * 
	 * @param inVO
	 * @return 1(성공)/0(실패)
	 */
	int doUpdate(T inVO);

	/**
	 * 목록조회
	 * 
	 * @param dto
	 * @return List<BoardVO>
	 */
	List<T> doRetrieve(DTO dto);
	
	/**
	 * 목록조회
	 * 
	 * @param dto
	 * @return List<BoardVO>
	 */
	List<T> doRetrieve(BoardDTO dto);

	/**
	 * 단건 저장
	 * 
	 * @param inVO
	 * @return 1(성공)/0(실패)
	 * @throws SQLException
	 */
	int doSave(T inVO);

	/**
	 * 단건 조회
	 * 
	 * @param inVO
	 * @return BoardVO
	 */
	T doSelectOne(T inVO);
	

}
