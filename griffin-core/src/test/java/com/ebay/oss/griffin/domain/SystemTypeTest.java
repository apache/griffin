package com.ebay.oss.griffin.domain;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ebay.oss.griffin.domain.SystemType;


public class SystemTypeTest {

    private static final String[] array = {"Bullseye", "GPS", "Hadoop", "PDS", "IDLS", "Pulsar", "Kafka", "Sojourner", "SiteSpeed", "EDW"};
    
    @Test
    public void testIndexOf() {
        for(int i = 0; i < array.length; i++) {
            assertEquals(i, SystemType.indexOf(array[i]));
        }
        assertEquals(-1, SystemType.indexOf("abcdefg"));
    }

    @Test
    public void testVal() {
        for (int i = 0; i < array.length; i++) {
            assertEquals(array[i], SystemType.val(i));
        }
        assertEquals(SystemType.val(105), "105");
    }
}
