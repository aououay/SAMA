package com.pcwk.ehr.air_quality.domain;

import java.util.Objects;

public class Pm_WarningVO {

	private String warnId;
	private String distNm;
	private String moveNm;
	private String issueItem;
	private String issueGbn;
	private int issueConc;
	private String issueDt;
	private String clearDt;
	private int sn;
	private int crtBy;
	private String crtDt;
	private int modBy;
	private String modDt;

	public Pm_WarningVO() {
		super();
	}

	public String getWarnId() {
		return warnId;
	}

	public void setWarnId(String warnId) {
		this.warnId = warnId;
	}

	public String getDistNm() {
		return distNm;
	}

	public void setDistNm(String distNm) {
		this.distNm = distNm;
	}

	public String getMoveNm() {
		return moveNm;
	}

	public void setMoveNm(String moveNm) {
		this.moveNm = moveNm;
	}

	public String getIssueItem() {
		return issueItem;
	}

	public void setIssueItem(String issueItem) {
		this.issueItem = issueItem;
	}

	public String getIssueGbn() {
		return issueGbn;
	}

	public void setIssueGbn(String issueGbn) {
		this.issueGbn = issueGbn;
	}

	public int getIssueConc() {
		return issueConc;
	}

	public void setIssueConc(int issueConc) {
		this.issueConc = issueConc;
	}

	public String getIssueDt() {
		return issueDt;
	}

	public void setIssueDt(String issueDt) {
		this.issueDt = issueDt;
	}

	public String getClearDt() {
		return clearDt;
	}

	public void setClearDt(String clearDt) {
		this.clearDt = clearDt;
	}

	public int getSn() {
		return sn;
	}

	public void setSn(int sn) {
		this.sn = sn;
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
		return Objects.hash(warnId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pm_WarningVO other = (Pm_WarningVO) obj;
		return Objects.equals(warnId, other.warnId);
	}

	@Override
	public String toString() {
		return "Pm_WarningVO [warnId=" + warnId + ", distNm=" + distNm + ", moveNm=" + moveNm + ", issueItem="
				+ issueItem + ", issueGbn=" + issueGbn + ", issueConc=" + issueConc + ", issueDt=" + issueDt
				+ ", clearDt=" + clearDt + ", sn=" + sn + ", crtBy=" + crtBy + ", crtDt=" + crtDt + ", modBy=" + modBy
				+ ", modDt=" + modDt + "]";
	}

	
	
	

}
