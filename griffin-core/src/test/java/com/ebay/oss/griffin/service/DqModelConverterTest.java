package com.ebay.oss.griffin.service;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ebay.oss.griffin.service.DqModelConverter;


public class DqModelConverterTest {

    private DqModelConverter converter;
    
    @Test
    public void test_voOf_null() {
        converter = new DqModelConverter();
        assertNull(converter.voOf(null));
    }
}
