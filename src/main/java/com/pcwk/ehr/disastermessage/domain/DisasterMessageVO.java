package com.pcwk.ehr.disastermessage.domain;

import com.pcwk.ehr.cmn.DTO;

public class DisasterMessageVO extends DTO {

    private Long disMesNum;
    private String disMesDt;
    private String disMesCon;
    private String disMesArea;
    private String disMesLevel;
    private String disMesType;
    private String disMesCreateDt;
    private String disMesUpdatedDt;

    public Long getDisMesNum() {
        return disMesNum;
    }

    public void setDisMesNum(Long disMesNum) {
        this.disMesNum = disMesNum;
    }

    public String getDisMesDt() {
        return disMesDt;
    }

    public void setDisMesDt(String disMesDt) {
        this.disMesDt = disMesDt;
    }

    public String getDisMesCon() {
        return disMesCon;
    }

    public void setDisMesCon(String disMesCon) {
        this.disMesCon = disMesCon;
    }

    public String getDisMesArea() {
        return disMesArea;
    }

    public void setDisMesArea(String disMesArea) {
        this.disMesArea = disMesArea;
    }

    public String getDisMesLevel() {
        return disMesLevel;
    }

    public void setDisMesLevel(String disMesLevel) {
        this.disMesLevel = disMesLevel;
    }

    public String getDisMesType() {
        return disMesType;
    }

    public void setDisMesType(String disMesType) {
        this.disMesType = disMesType;
    }

    public String getDisMesCreateDt() {
        return disMesCreateDt;
    }

    public void setDisMesCreateDt(String disMesCreateDt) {
        this.disMesCreateDt = disMesCreateDt;
    }

    public String getDisMesUpdatedDt() {
        return disMesUpdatedDt;
    }

    public void setDisMesUpdatedDt(String disMesUpdatedDt) {
        this.disMesUpdatedDt = disMesUpdatedDt;
    }

    // toString method for displaying object info
    @Override
    public String toString() {
        return "DisasterMessageVO{" +
                "disMesNum=" + disMesNum +
                ", disMesDt='" + disMesDt + '\'' +
                ", disMesCon='" + disMesCon + '\'' +
                ", disMesArea='" + disMesArea + '\'' +
                ", disMesLevel='" + disMesLevel + '\'' +
                ", disMesType='" + disMesType + '\'' +
                ", disMesCreateDt='" + disMesCreateDt + '\'' +
                ", disMesUpdatedDt='" + disMesUpdatedDt + '\'' +
                '}';
    }
}
