package com.ebay.oss.griffin.common;

import org.junit.Test;

import com.ebay.oss.griffin.common.Pair;

import java.util.*;

import static org.junit.Assert.*;

public class CommonResourceTest {

    @Test
    public void testPair() {
        Pair pair = new Pair("key", "value");
        Pair pair1 = new Pair("key", "value");
        Pair pair2 = new Pair("key", "notVal");
        Pair pair3 = new Pair("notKey", "notVal");

        assertEquals(pair.hashCode(), pair1.hashCode());
        assertTrue(pair.hashCode() != pair2.hashCode());

        assertTrue(pair.equals(pair));
        assertFalse(pair.equals(null));
        assertTrue(pair.equals(pair1));
        assertFalse(pair.equals(pair2));
        assertFalse(pair.equals(pair3));
        assertFalse(pair.equals("key value"));
        assertTrue(new Pair(null, null).equals(new Pair(null, null)));
        assertFalse(new Pair(null, null).equals(new Pair("key", null)));
        assertTrue(new Pair("key", null).equals(new Pair("key", null)));
        assertFalse(new Pair("key", null).equals(new Pair("key", "val")));
    }
}
