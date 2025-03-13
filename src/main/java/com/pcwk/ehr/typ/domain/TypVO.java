package com.pcwk.ehr.typ.domain;

public class TypVO {

    // 필드 (멤버 변수)
    private int typNum;
    private String typName;
    private String typSt;
    private String typEd;
    private int typMinPressure;
    private int typMaxWind;
    private String typKorName;
    private String typEff;


    public TypVO() {
    }


    public int getTypNum() {
        return typNum;
    }

    public void setTypNum(int typNum) {
        this.typNum = typNum;
    }

    public String getTypName() {
        return typName;
    }

    public void setTypName(String typName) {
        this.typName = typName;
    }

    public String getTypSt() {
        return typSt;
    }

    public void setTypSt(String typSt) {
        this.typSt = typSt;
    }

    public String getTypEd() {
        return typEd;
    }

    public void setTypEd(String typEd) {
        this.typEd = typEd;
    }

    public int getTypMinPressure() {
        return typMinPressure;
    }

    public void setTypMinPressure(int typMinPressure) {
        this.typMinPressure = typMinPressure;
    }

    public int getTypMaxWind() {
        return typMaxWind;
    }

    public void setTypMaxWind(int typMaxWind) {
        this.typMaxWind = typMaxWind;
    }

    public String getTypKorName() {
        return typKorName;
    }

    public void setTypKorName(String typKorName) {
        this.typKorName = typKorName;
    }

    public String getTypEff() {
        return typEff;
    }

    public void setTypEff(String typEff) {
        this.typEff = typEff;
    }

    // toString() 메서드 (객체 내용을 출력할 때 사용)
    @Override
    public String toString() {
        return "TypVO{" +
                "typNum=" + typNum +
                ", typName='" + typName + '\'' +
                ", typSt='" + typSt + '\'' +
                ", typEd='" + typEd + '\'' +
                ", typMinPressure=" + typMinPressure +
                ", typMaxWind=" + typMaxWind +
                ", typKorName='" + typKorName + '\'' +
                ", typEff=" + typEff +
                '}';
    }
}
