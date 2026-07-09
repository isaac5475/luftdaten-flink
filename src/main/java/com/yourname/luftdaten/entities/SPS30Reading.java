package com.yourname.luftdaten.entities;

public class SPS30Reading extends SensorReading {
    private Double p1, p2, p0;
    private Double p4;
    private Double N10, N4, N25, N05, N1;
    private Double TS;

    public SPS30Reading() {
        super();
    }

    public SPS30Reading(SensorReading sensorReading) {
        super(sensorReading);
    }

    public SPS30Reading(SPS30Reading sensorReading) {
        this((SensorReading) sensorReading);
        setP0(sensorReading.getP0());
        setP1(sensorReading.getP1());
        setP2(sensorReading.getP2());
        setP4(sensorReading.getP4());
        setN10(sensorReading.getN10());
        setN4(sensorReading.getN4());
        setN25(sensorReading.getN25());
        setN05(sensorReading.getN05());
        setN1(sensorReading.getN1());
        setTS(sensorReading.getTS());
    }

    public Double getP1() {
        return p1;
    }

    public void setP1(Double p1) {
        this.p1 = p1;
    }

    public Double getP2() {
        return p2;
    }

    public void setP2(Double p2) {
        this.p2 = p2;
    }

    public Double getP0() {
        return p0;
    }

    public void setP0(Double p0) {
        this.p0 = p0;
    }

    public Double getP4() {
        return p4;
    }

    public void setP4(Double p4) {
        this.p4 = p4;
    }

    public Double getN10() {
        return N10;
    }

    public void setN10(Double n10) {
        N10 = n10;
    }

    public Double getN4() {
        return N4;
    }

    public void setN4(Double n4) {
        N4 = n4;
    }

    public Double getN25() {
        return N25;
    }

    public void setN25(Double n25) {
        N25 = n25;
    }

    public Double getN05() {
        return N05;
    }

    public void setN05(Double n05) {
        N05 = n05;
    }

    public Double getN1() {
        return N1;
    }

    public void setN1(Double n1) {
        N1 = n1;
    }

    public Double getTS() {
        return TS;
    }

    public void setTS(Double TS) {
        this.TS = TS;
    }

    @Override
    public String toString() {
        return String.format("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s",
                getSensor_id(), getSensorType(), getLocation(), getLat(), getLon(), getTimestamp(), getP1(), getP4(),
                getP2(), getP0(), getN10(), getN4(), getN25(), getN1(), getN25(), getTS());
    }
}
