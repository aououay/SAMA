package com.pcwk.ehr.air_quality.domain;

import java.util.Objects;

public class Air_QualityVO {

	private long aqId;
	private String guCd;
	private String guNm;
	private String msAddr;
	private double msLat;
	private double msLon;
	private int sortOrd;
	private String caiGrade;
	private int cai;
	private int pm25Conc;
	private int pm10Conc;
	private double o3Conc;
	private String msDt;
	private int crtBy;
	private String crtDt;
	private int modBy;
	private String modDt;

	public Air_QualityVO() {
		super();
	}

	public long getAqId() {
		return aqId;
	}

	public void setAqId(long aqId) {
		this.aqId = aqId;
	}

	public String getGuCd() {
		return guCd;
	}

	public void setGuCd(String guCd) {
		this.guCd = guCd;
	}

	public String getGuNm() {
		return guNm;
	}

	public void setGuNm(String guNm) {
		this.guNm = guNm;
	}

	public String getMsAddr() {
		return msAddr;
	}

	public void setMsAddr(String msAddr) {
		this.msAddr = msAddr;
	}

	public double getMsLat() {
		return msLat;
	}

	public void setMsLat(double msLat) {
		this.msLat = msLat;
	}

	public double getMsLon() {
		return msLon;
	}

	public void setMsLon(double msLon) {
		this.msLon = msLon;
	}

	public int getSortOrd() {
		return sortOrd;
	}

	public void setSortOrd(int sortOrd) {
		this.sortOrd = sortOrd;
	}

	public String getCaiGrade() {
		return caiGrade;
	}

	public void setCaiGrade(String caiGrade) {
		this.caiGrade = caiGrade;
	}

	public int getCai() {
		return cai;
	}

	public void setCai(int cai) {
		this.cai = cai;
	}

	public int getPm25Conc() {
		return pm25Conc;
	}

	public void setPm25Conc(int pm25Conc) {
		this.pm25Conc = pm25Conc;
	}

	public int getPm10Conc() {
		return pm10Conc;
	}

	public void setPm10Conc(int pm10Conc) {
		this.pm10Conc = pm10Conc;
	}

	public double getO3Conc() {
		return o3Conc;
	}

	public void setO3Conc(double o3Conc) {
		this.o3Conc = o3Conc;
	}

	public String getMsDt() {
		return msDt;
	}

	public void setMsDt(String msDt) {
		this.msDt = msDt;
	}

	public int getCrtBy() {
		return crtBy;
	}

	public void setCrtBy(int crtBy) {
		this.crtBy = crtBy;
	}

	public String getCrtDt() {
		return crtDt;
	}

	public void setCrtDt(String crtDt) {
		this.crtDt = crtDt;
	}

	public int getModBy() {
		return modBy;
	}

	public void setModBy(int modBy) {
		this.modBy = modBy;
	}

	public String getModDt() {
		return modDt;
	}

	public void setModDt(String modDt) {
		this.modDt = modDt;
	}

	@Override
	public int hashCode() {
		return Objects.hash(aqId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Air_QualityVO other = (Air_QualityVO) obj;
		return aqId == other.aqId;
	}

	@Override
	public String toString() {
		return "Air_QualityVO [aqId=" + aqId + ", guCd=" + guCd + ", guNm=" + guNm + ", msAddr=" + msAddr + ", msLat="
				+ msLat + ", msLon=" + msLon + ", sortOrd=" + sortOrd + ", caiGrade=" + caiGrade + ", cai=" + cai
				+ ", pm25Conc=" + pm25Conc + ", pm10Conc=" + pm10Conc + ", o3Conc=" + o3Conc + ", msDt=" + msDt
				+ ", crtBy=" + crtBy + ", crtDt=" + crtDt + ", modBy=" + modBy + ", modDt=" + modDt + "]";
	}

}
