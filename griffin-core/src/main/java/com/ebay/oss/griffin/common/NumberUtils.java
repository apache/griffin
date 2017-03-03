package com.ebay.oss.griffin.common;

import org.springframework.util.StringUtils;

public class NumberUtils {

    public static int parseInt(Object o) {
        if (o == null) {
            return -1;
        }
        
        String s = o.toString();
        if(StringUtils.isEmpty(s) ) {
            return -1;
        }
        
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            return -1;
        }
    }

    public static long parseLong(Object o) {
        if (o == null) {
            return -1;
        }
        
        String s = o.toString();
        if(StringUtils.isEmpty(s) ) {
            return -1;
        }
        
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return -1;
        }
    }

}
