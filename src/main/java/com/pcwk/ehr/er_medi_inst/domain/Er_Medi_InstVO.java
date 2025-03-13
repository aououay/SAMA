package com.pcwk.ehr.er_medi_inst.domain;

import java.util.Objects;

public class Er_Medi_InstVO {

	private String hpid;
	private String guCd;
	private String dutyNm;
	private String dutyAddr;
	private String dutyTel;
	private String erTel;
	private int crtBy;
	private String crtDt;
	private int modBy;
	private String modDt;

	public Er_Medi_InstVO() {
		super();
	}

	public String getHpid() {
		return hpid;
	}

	public void setHpid(String hpid) {
		this.hpid = hpid;
	}

	public String getGuCd() {
		return guCd;
	}

	public void setGuCd(String guCd) {
		this.guCd = guCd;
	}

	public String getDutyNm() {
		return dutyNm;
	}

	public void setDutyNm(String dutyNm) {
		this.dutyNm = dutyNm;
	}

	public String getDutyAddr() {
		return dutyAddr;
	}

	public void setDutyAddr(String dutyAddr) {
		this.dutyAddr = dutyAddr;
	}

	public String getDutyTel() {
		return dutyTel;
	}

	public void setDutyTel(String dutyTel) {
		this.dutyTel = dutyTel;
	}

	public String getErTel() {
		return erTel;
	}

	public void setErTel(String erTel) {
		this.erTel = erTel;
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
		return Objects.hash(hpid);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Er_Medi_InstVO other = (Er_Medi_InstVO) obj;
		return Objects.equals(hpid, other.hpid);
	}

	@Override
	public String toString() {
		return "Er_Medi_InstVO [hpid=" + hpid + ", guCd=" + guCd + ", dutyNm=" + dutyNm + ", dutyAddr=" + dutyAddr
				+ ", dutyTel=" + dutyTel + ", erTel=" + erTel + ", crtBy=" + crtBy + ", crtDt=" + crtDt + ", modBy="
				+ modBy + ", modDt=" + modDt + "]";
	}

}
