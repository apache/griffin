package org.apache.griffin.measure.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5工具类
 *
 * @author Mancy Chan
 * @version 1.0
 */
public class Md5Util {

    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };

    private Md5Util() {
    }

    /**
     * @param s 原始字符串
     * @return 使用MD5处理后的字符串
     */
    public static String md5(String s) {

        try {
            byte[] bytes = s.getBytes();

            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(bytes);
            byte[] mdBytes = messageDigest.digest(); // 获得密文

            // 把密文转换成十六进制的字符串
            int j = mdBytes.length;
            char[] str = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = mdBytes[i];
                str[k++] = HEX_DIGITS[byte0 >>> 4 & 0xf];
                str[k++] = HEX_DIGITS[byte0 & 0xf];
            }
            return new String(str);
        } catch (NoSuchAlgorithmException ex) {

            return null;
        }
    }

//    public static void main(String[] args) {
//        String sysId = "umc";
//        String timestamp = "1481256047966";
//        String umcKey = "AABBDDCC";
//
//        System.out.println(md5(sysId + timestamp + umcKey));
//    }

}