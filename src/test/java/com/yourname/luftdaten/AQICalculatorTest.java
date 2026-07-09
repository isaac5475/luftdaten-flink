package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AQICalculatorTest {

    @Test
    void testAqiP1Boundary() {
        // pm10 = 55 => truncated to 55 => should map to 51 (start of second band)
        int aqi = AQICalculator.aqiP1(55.0);
        assertEquals(51, aqi);
    }

    @Test
    void testAqiP2Boundary() {
        // pm2.5 = 12.1 => truncated to 12.1 (one decimal) => start of second band -> 51
        int aqi = AQICalculator.aqiP2(12.1);
        assertEquals(51, aqi);
    }

    @Test
    void testAqiCombined() {
        // choose values where pm10 yields lower and pm2.5 yields higher
        int combined = AQICalculator.aqi(55.0, 12.1);
        assertEquals(Math.max(AQICalculator.aqiP1(55.0), AQICalculator.aqiP2(12.1)), combined);
    }
}

