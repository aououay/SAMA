package com.pcwk.ehr.disastermessage.domain;

public class DisasterCountVO {

    public int dustCount;
    public int heatWaveCount;
    public int heavyRainCount;
    public int coldWaveCount;
    public int heavySnowCount;
    public int wildfireCount;
    public int stormCount;
    public int epidemicCount;
    public int landslideCount;

    public int getDustCount() {
        return dustCount;
    }

    public void setDustCount(int dustCount) {
        this.dustCount = dustCount;
    }

    public int getHeatWaveCount() {
        return heatWaveCount;
    }

    public void setHeatWaveCount(int heatWaveCount) {
        this.heatWaveCount = heatWaveCount;
    }

    public int getHeavyRainCount() {
        return heavyRainCount;
    }

    public void setHeavyRainCount(int heavyRainCount) {
        this.heavyRainCount = heavyRainCount;
    }

    public int getColdWaveCount() {
        return coldWaveCount;
    }

    public void setColdWaveCount(int coldWaveCount) {
        this.coldWaveCount = coldWaveCount;
    }

    public int getHeavySnowCount() {
        return heavySnowCount;
    }

    public void setHeavySnowCount(int heavySnowCount) {
        this.heavySnowCount = heavySnowCount;
    }

    public int getWildfireCount() {
        return wildfireCount;
    }

    public void setWildfireCount(int wildfireCount) {
        this.wildfireCount = wildfireCount;
    }

    public int getStormCount() {
        return stormCount;
    }

    public void setStormCount(int stormCount) {
        this.stormCount = stormCount;
    }

    public int getEpidemicCount() {
        return epidemicCount;
    }

    public void setEpidemicCount(int epidemicCount) {
        this.epidemicCount = epidemicCount;
    }

    public int getLandslideCount() {
        return landslideCount;
    }

    public void setLandslideCount(int landslideCount) {
        this.landslideCount = landslideCount;
    }

    @Override
    public String toString() {
        return "DisasterCountVO{" +
                "dustCount=" + dustCount +
                ", heatWaveCount=" + heatWaveCount +
                ", heavyRainCount=" + heavyRainCount +
                ", coldWaveCount=" + coldWaveCount +
                ", heavySnowCount=" + heavySnowCount +
                ", wildfireCount=" + wildfireCount +
                ", stormCount=" + stormCount +
                ", epidemicCount=" + epidemicCount +
                ", landslideCount=" + landslideCount +
                '}';
    }
}
