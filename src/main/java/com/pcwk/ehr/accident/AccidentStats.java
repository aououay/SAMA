package com.pcwk.ehr.accident;

public class AccidentStats {
	
	private int acdntCnt; 			// 사고 건수
    private int dcsdCnt; 			// 사망자 수
    private String statsCrtrStaDt;  // 통계 시작일
    private String statsCrtrEndDt;  // 통계 종료일
    private String month; 			// 월별 통계 
    private int totalAcdntCnt; 		// 월별 사고 건수 합계
    private int totalDcsdCnt; 		// 월별 사망자 수 합계
    
    public AccidentStats() {
    	
    }

    public int getAcdntCnt() {
    	return acdntCnt; 
    }
    public void setAcdntCnt(int acdntCnt) { 
    	this.acdntCnt = acdntCnt; 
    }

    public int getDcsdCnt() { 
    	return dcsdCnt; 
    }
    public void setDcsdCnt(int dcsdCnt) { 
    	this.dcsdCnt = dcsdCnt;
    }

    public String getStatsCrtrStaDt() { 
    	return statsCrtrStaDt; 
    }
    public void setStatsCrtrStaDt(String statsCrtrStaDt) { 
    	this.statsCrtrStaDt = statsCrtrStaDt; 
    }

    public String getStatsCrtrEndDt() { 
    	return statsCrtrEndDt; 
    }
    public void setStatsCrtrEndDt(String statsCrtrEndDt) {
    	this.statsCrtrEndDt = statsCrtrEndDt; 
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
}
