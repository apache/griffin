package org.apache.griffin.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by xiangrchen on 7/13/17.
 */
public class PropertiesOperate {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesOperate.class);

    public static Properties getProperties(String propFileName) throws IOException {
        InputStream inputStream = null;
        Properties prop = new Properties();
        ;
        try {
//            String propFileName = "sparkJob.properties";
            inputStream = PropertiesOperate.class.getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        } catch (Exception e) {
            LOGGER.info("Exception: " + e);
        } finally {
            inputStream.close();
        }
        return prop;
    }

}
