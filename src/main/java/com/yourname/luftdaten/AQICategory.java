package com.yourname.luftdaten;

public enum AQICategory {
    GOOD(0, 50),
    MODERATE(51, 100),
    UNHEALTHY_FOR_SENSITIVE(101, 150),
    UNHEALTHY(151, 200),
    VERY_UNHEALTHY(201, 300),
    HAZARDOUS(301, 400),
    BEYOND_HAZARDOUS(401, 500);

    private final int lo, hi;

    AQICategory(int lo, int hi) {
        this.lo = lo;
        this.hi = hi;
    }

    public static AQICategory of(int aqi) {
        for (AQICategory cat : values()) {
            if (aqi >= cat.lo && aqi <= cat.hi)
                return cat;
        }
        throw new IllegalArgumentException("AQI out of range: " + aqi);
    }

    public boolean isAtLeast(AQICategory threshold) {
        return this.lo >= threshold.lo;
    }
}