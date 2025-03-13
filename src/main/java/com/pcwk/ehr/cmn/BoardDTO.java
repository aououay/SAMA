package com.pcwk.ehr.cmn;

public class BoardDTO {
	private int no; //글 번호
	private int totalCnt; //총 글수
	
	private int pageSize; //페이지 사이즈
	private int pageNo;   //페이지 번호
	
	private String optionDiv;//추가 검색

	public BoardDTO() {
		super();
	}

	public int getNo() {
		return no;
	}

	public void setNo(int no) {
		this.no = no;
	}

	public int getTotalCnt() {
		return totalCnt;
	}

	public void setTotalCnt(int totalCnt) {
		this.totalCnt = totalCnt;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	public String getOptionDiv() {
		return optionDiv;
	}

	public void setOptionDiv(String optionDiv) {
		this.optionDiv = optionDiv;
	}

	@Override
	public String toString() {
		return "BoardDTO [no=" + no + ", totalCnt=" + totalCnt + ", pageSize=" + pageSize + ", pageNo=" + pageNo
				+ ", optionDiv=" + optionDiv + "]";
	}
	
	
	
	
}
