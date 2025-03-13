package com.pcwk.ehr.hanpa.domain;

import com.pcwk.ehr.cmn.DTO;

public class HanpaVO extends DTO {

    private int seqNo;
    private String sgg;
    private String fcltyType;
    private String fcltNm;
    private String opHour;
    private String address;
    private String contactNm;
    private String opType;
    
    
    
	public int getSeqNo() {
		return seqNo;
	}
	public void setSeqNo(int seqNo) {
		this.seqNo = seqNo;
	}
	public String getSgg() {
		return sgg;
	}
	public void setSgg(String sgg) {
		this.sgg = sgg;
	}
	public String getFcltyType() {
		return fcltyType;
	}
	public void setFcltyType(String fcltyType) {
		this.fcltyType = fcltyType;
	}
	public String getFcltNm() {
		return fcltNm;
	}
	public void setFcltNm(String fcltNm) {
		this.fcltNm = fcltNm;
	}
	public String getOpHour() {
		return opHour;
	}
	public void setOpHour(String opHour) {
		this.opHour = opHour;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getContactNm() {
		return contactNm;
	}
	public void setContactNm(String contactNm) {
		this.contactNm = contactNm;
	}
	public String getOpType() {
		return opType;
	}
	public void setOpType(String opType) {
		this.opType = opType;
	}
	@Override
	public String toString() {
		return "HanpaVO [seqNo=" + seqNo + ", sgg=" + sgg + ", fcltyType=" + fcltyType + ", fcltNm=" + fcltNm
				+ ", opHour=" + opHour + ", address=" + address + ", contactNm=" + contactNm + ", opType=" + opType
				+ "]";
	}
    
    
}
