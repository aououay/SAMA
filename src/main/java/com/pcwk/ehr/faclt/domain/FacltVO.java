package com.pcwk.ehr.faclt.domain;

import com.pcwk.ehr.cmn.DTO;

public class FacltVO extends DTO {

	private String fcltCd;
	private String sggCd;
	private String seCd;
	private String fcltNm;
	private String fcltDsgnDay;
	private String fcltSeCd;
	private String emdCd;
	private String emdNm;
	private String fcltAddrLotno;
	private String fcltScl;
	private String sclUnit;
	private String fcltAddrRoad;
	private int lotProvin;
	private int lotMin;
	private int lotSec;
	private int latProvin;
	private int latMin;
	private int latSec;
	private String grndUdgdSe;
	private int shntPsbltyNope;
	private String opnYn;
	private String mngInstNm;
	private String ortmUtlzType;
	private String mngInstTelno;
	private String roadNmCd;

    public double getDecimalLongitude() {
        return lotProvin + (lotMin / 60.0) + (lotSec / 3600.0);
    }

    public double getDecimalLatitude() {
        return latProvin + (latMin / 60.0) + (latSec / 3600.0);
    }

	// Getter 및 Setter 메서드
	public String getFcltCd() {
		return fcltCd;
	}

	public void setFcltCd(String fcltCd) {
		this.fcltCd = fcltCd;
	}

	public String getSggCd() {
		return sggCd;
	}

	public void setSggCd(String sggCd) {
		this.sggCd = sggCd;
	}

	public String getSeCd() {
		return seCd;
	}

	public void setSeCd(String seCd) {
		this.seCd = seCd;
	}

	public String getFcltNm() {
		return fcltNm;
	}

	public void setFcltNm(String fcltNm) {
		this.fcltNm = fcltNm;
	}

	public String getFcltDsgnDay() {
		return fcltDsgnDay;
	}

	public void setFcltDsgnDay(String fcltDsgnDay) {
		this.fcltDsgnDay = fcltDsgnDay;
	}

	public String getFcltSeCd() {
		return fcltSeCd;
	}

	public void setFcltSeCd(String fcltSeCd) {
		this.fcltSeCd = fcltSeCd;
	}

	public String getEmdCd() {
		return emdCd;
	}

	public void setEmdCd(String emdCd) {
		this.emdCd = emdCd;
	}

	public String getEmdNm() {
		return emdNm;
	}

	public void setEmdNm(String emdNm) {
		this.emdNm = emdNm;
	}

	public String getFcltAddrLotno() {
		return fcltAddrLotno;
	}

	public void setFcltAddrLotno(String fcltAddrLotno) {
		this.fcltAddrLotno = fcltAddrLotno;
	}

	public String getFcltScl() {
		return fcltScl;
	}

	public void setFcltScl(String fcltScl) {
		this.fcltScl = fcltScl;
	}

	public String getSciUnit() {
		return sclUnit;
	}

	public void setSciUnit(String sclUnit) {
		this.sclUnit = sclUnit;
	}

	public String getFcltAddrRoad() {
		return fcltAddrRoad;
	}

	public void setFcltAddrRoad(String fcltAddrRoad) {
		this.fcltAddrRoad = fcltAddrRoad;
	}

	public int getLotProvin() {
		return lotProvin;
	}

	public void setLotProvin(int lotProvin) {
		this.lotProvin = lotProvin;
	}

	public int getLotMin() {
		return lotMin;
	}

	public void setLotMin(int lotMin) {
		this.lotMin = lotMin;
	}

	public int getLotSec() {
		return lotSec;
	}

	public void setLotSec(int lotSec) {
		this.lotSec = lotSec;
	}

	public int getLatProvin() {
		return latProvin;
	}

	public void setLatProvin(int latProvin) {
		this.latProvin = latProvin;
	}

	public int getLatMin() {
		return latMin;
	}

	public void setLatMin(int latMin) {
		this.latMin = latMin;
	}

	public int getLatSec() {
		return latSec;
	}

	public void setLatSec(int latSec) {
		this.latSec = latSec;
	}

	public String getGrndUdgdSe() {
		return grndUdgdSe;
	}

	public void setGrndUdgdSe(String grndUdgdSe) {
		this.grndUdgdSe = grndUdgdSe;
	}

	public int getShntPsbltyNope() {
		return shntPsbltyNope;
	}

	public void setShntPsbltyNope(int shntPsblyNope) {
		this.shntPsbltyNope = shntPsblyNope;
	}

	public String getOpnYn() {
		return opnYn;
	}

	public void setOpnYn(String opnYn) {
		this.opnYn = opnYn;
	}

	public String getMngInstNm() {
		return mngInstNm;
	}

	public void setMngInstNm(String mngInstNm) {
		this.mngInstNm = mngInstNm;
	}

	public String getOrtmUtlzType() {
		return ortmUtlzType;
	}

	public void setOrtmUtlzType(String ortmUtlzType) {
		this.ortmUtlzType = ortmUtlzType;
	}

	public String getMngInstTelno() {
		return mngInstTelno;
	}

	public void setMngInstTelno(String mngInstTelno) {
		this.mngInstTelno = mngInstTelno;
	}

	public String getRoadNmCd() {
		return roadNmCd;
	}

	public void setRoadNmCd(String roadNmCd) {
		this.roadNmCd = roadNmCd;
	}
	
	public String getFormattedFcltDsgnDay() {
	    if (fcltDsgnDay != null && fcltDsgnDay.length() == 8) {
	        String year = fcltDsgnDay.substring(0, 4);
	        String month = fcltDsgnDay.substring(4, 6);
	        String day = fcltDsgnDay.substring(6, 8);
	        return year + "-" + month + "-" + day;
	    }
	    return fcltDsgnDay; // 변환할 수 없으면 원본 값 유지
	}
	
	


	@Override
	public String toString() {
	    return "FacltVO [fcltCd=" + fcltCd + ", sggCd=" + sggCd + ", seCd=" + seCd + ", fcltNm=" + fcltNm
	            + ", fcltDsgnDay=" + fcltDsgnDay + ", fcltSeCd=" + fcltSeCd + ", emdCd=" + emdCd + ", emdNm=" + emdNm
	            + ", fcltAddrLotno=" + fcltAddrLotno + ", fcltScl=" + fcltScl + ", sclUnit=" + sclUnit
	            + ", fcltAddrRoad=" + fcltAddrRoad + ", lotProvin=" + lotProvin + ", lotMin=" + lotMin + ", lotSec="
	            + lotSec + ", latProvin=" + latProvin + ", latMin=" + latMin + ", latSec=" + latSec + ", grndUdgdSe="
	            + grndUdgdSe + ", shntPsbltyNope=" + shntPsbltyNope + ", opnYn=" + opnYn + ", mngInstNm=" + mngInstNm
	            + ", ortmUtlzType=" + ortmUtlzType + ", mngInstTelno=" + mngInstTelno + ", roadNmCd=" + roadNmCd
	            + ", decimalLatitude=" + getDecimalLatitude() + ", decimalLongitude=" + getDecimalLongitude()
	            + " ]";
	}


}
