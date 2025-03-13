package com.pcwk.ehr.accident;

public class YearlyComparisonDTO {
	private String month;
    private int totalAcdntCnt; //올해 사고 건수
    private int totalDcsdCnt; //올해 사망자 수
    private int prevTotalAcdntCnt; //전년도 사고 건수
    private int prevTotalDcsdCnt; //전년도 사망자 수
    
	public YearlyComparisonDTO() {
		super();
	}
	
	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public int getTotalAcdntCnt() {
		return totalAcdntCnt;
	}

	public void setTotalAcdntCnt(int totalAcdntCnt) {
		this.totalAcdntCnt = totalAcdntCnt;
	}

	public int getTotalDcsdCnt() {
		return totalDcsdCnt;
	}

	public void setTotalDcsdCnt(int totalDcsdCnt) {
		this.totalDcsdCnt = totalDcsdCnt;
	}

	public int getPrevTotalAcdntCnt() {
		return prevTotalAcdntCnt;
	}

	public void setPrevTotalAcdntCnt(int prevTotalAcdntCnt) {
		this.prevTotalAcdntCnt = prevTotalAcdntCnt;
	}

	public int getPrevTotalDcsdCnt() {
		return prevTotalDcsdCnt;
	}

	public void setPrevTotalDcsdCnt(int prevTotalDcsdCnt) {
		this.prevTotalDcsdCnt = prevTotalDcsdCnt;
	}  
    
}
