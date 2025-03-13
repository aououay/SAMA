package com.pcwk.ehr.insurance.domain;

public class InsVO {

	private String insSggCd;
	private String insSggNm;
	private String insClient;
	private String insStartDt;
	private String insEndDt;
	private String insCompany;
	private String insMethod;
	private String insUrl;
	
	private String grntSggYearCd;
	private String grntCd;
	private String grntNm;
	private String grntContent;
	private int    grntAmount;
	
	public InsVO() {
		super();
	}

	public InsVO(String insSggCd, String insSggNm, String insClient, String insStartDt, String insEndDt,
			String insCompany, String insMethod, String insUrl, String grntSggYearCd, String grntCd, String grntNm,
			String grntContent, int grntAmount) {
		super();
		this.insSggCd = insSggCd;
		this.insSggNm = insSggNm;
		this.insClient = insClient;
		this.insStartDt = insStartDt;
		this.insEndDt = insEndDt;
		this.insCompany = insCompany;
		this.insMethod = insMethod;
		this.insUrl = insUrl;
		this.grntSggYearCd = grntSggYearCd;
		this.grntCd = grntCd;
		this.grntNm = grntNm;
		this.grntContent = grntContent;
		this.grntAmount = grntAmount;
	}

	public String getInsSggCd() {
		return insSggCd;
	}

	public void setInsSggCd(String insSggCd) {
		this.insSggCd = insSggCd;
	}

	public String getInsSggNm() {
		return insSggNm;
	}

	public void setInsSggNm(String insSggNm) {
		this.insSggNm = insSggNm;
	}

	public String getInsClient() {
		return insClient;
	}

	public void setInsClient(String insClient) {
		this.insClient = insClient;
	}

	public String getInsStartDt() {
		return insStartDt;
	}

	public void setInsStartDt(String insStartDt) {
		this.insStartDt = insStartDt;
	}

	public String getInsEndDt() {
		return insEndDt;
	}

	public void setInsEndDt(String insEndDt) {
		this.insEndDt = insEndDt;
	}

	public String getInsCompany() {
		return insCompany;
	}

	public void setInsCompany(String insCompany) {
		this.insCompany = insCompany;
	}

	public String getInsMethod() {
		return insMethod;
	}

	public void setInsMethod(String insMethod) {
		this.insMethod = insMethod;
	}

	public String getInsUrl() {
		return insUrl;
	}

	public void setInsUrl(String insUrl) {
		this.insUrl = insUrl;
	}

	public String getGrntSggYearCd() {
		return grntSggYearCd;
	}

	public void setGrntSggYearCd(String grntSggYearCd) {
		this.grntSggYearCd = grntSggYearCd;
	}

	public String getGrntCd() {
		return grntCd;
	}

	public void setGrntCd(String grntCd) {
		this.grntCd = grntCd;
	}

	public String getGrntNm() {
		return grntNm;
	}

	public void setGrntNm(String grntNm) {
		this.grntNm = grntNm;
	}

	public String getGrntContent() {
		return grntContent;
	}

	public void setGrntContent(String grntContent) {
		this.grntContent = grntContent;
	}

	public int getGrntAmount() {
		return grntAmount;
	}

	public void setGrntAmount(int grntAmount) {
		this.grntAmount = grntAmount;
	}

	@Override
	public String toString() {
		return "InsVO [insSggCd=" + insSggCd + ", insSggNm=" + insSggNm + ", insClient=" + insClient + ", insStartDt="
				+ insStartDt + ", insEndDt=" + insEndDt + ", insCompany=" + insCompany + ", insMethod=" + insMethod
				+ ", insUrl=" + insUrl + ", grntSggYearCd=" + grntSggYearCd + ", grntCd=" + grntCd + ", grntNm="
				+ grntNm + ", grntContent=" + grntContent + ", grntAmount=" + grntAmount + "]";
	}

	
}
