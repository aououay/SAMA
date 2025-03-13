package com.pcwk.ehr.ewsf.domain;

import java.util.Objects;

public class EwsfVO {
    private String sgg;        // 시군구
    private String sggFclt;    // 시설명
    private String pumpTon;    // 펌프 용량 (단위: 톤)

    // 기본 생성자
    public EwsfVO() {

    }

    public String getSgg() {
        return sgg;
    }

    public void setSgg(String sgg) {
        this.sgg = sgg;
    }

    public String getSggFclt() {
        return sggFclt;
    }

    public void setSggFclt(String sggFclt) {
        this.sggFclt = sggFclt;
    }

    public String getPumpTon() {
        return pumpTon;
    }

    public void setPumpTon(String pumpTon) {
        this.pumpTon = pumpTon;
    }

    // toString() : 객체 정보 출력
    @Override
    public String toString() {
        return "EwsfVO{" +
                "sgg='" + sgg + '\'' +
                ", sggFclt='" + sggFclt + '\'' +
                ", pumpTon='" + pumpTon + '\'' +
                '}';
    }
}
