package com.yourname.luftdaten;

public class AQICalculator {

    // PM10 breakpoints (p1)
    private static final double[][] PM10_BP = {
            {0, 54,    0,  50},
            {55, 154,  51, 100},
            {155, 254, 101, 150},
            {255, 354, 151, 200},
            {355, 424, 201, 300},
            {425, 504, 301, 400},
            {505, 604, 401, 500}
    };

    // PM2.5 breakpoints (p2)
    private static final double[][] PM25_BP = {
            {0.0,  12.0,  0,  50},
            {12.1, 35.4,  51, 100},
            {35.5, 55.4,  101, 150},
            {55.5, 150.4, 151, 200},
            {150.5, 250.4, 201, 300},
            {250.5, 350.4, 301, 400},
            {350.5, 500.4, 401, 500}
    };

    public static int calcAQI(double concentration, double[][] breakpoints) {
        for (double[] bp : breakpoints) {
            double cLo = bp[0], cHi = bp[1];
            double iLo = bp[2], iHi = bp[3];
            if (concentration >= cLo && concentration <= cHi) {
                return (int) Math.round(
                        ((iHi - iLo) / (cHi - cLo)) * (concentration - cLo) + iLo
                );
            }
        }
        return 500; // beyond scale
    }

    public static int aqiP1(double pm10) {
        double truncated = Math.floor(pm10); // truncate to integer
        return calcAQI(truncated, PM10_BP);
    }

    public static int aqiP2(double pm25) {
        double truncated = Math.floor(pm25 * 10) / 10.0; // truncate to 1 decimal
        return calcAQI(truncated, PM25_BP);
    }

    public static int aqi(double pm10, double pm25) {
        return Math.max(aqiP1(pm10), aqiP2(pm25));
    }
}