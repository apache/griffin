package org.apache.griffin.core.metastore.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Author: qwang8
 * Date:   2019-06-25.
 */
class HiveConnectMgr {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(HiveConnectMgr.class);

    private static HiveConnectMgr hiveConnectMgr;

    @Value("${hive.jdbc.className}")
    private String hiveClassName;

    @Value("${hive.jdbc.url}")
    private String hiveURL;

    private HiveConnectMgr() {}

    private Connection conn;

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    static HiveConnectMgr getHiveConnectionMgr() {
        if (hiveConnectMgr == null)
            hiveConnectMgr = new HiveConnectMgr();
        return hiveConnectMgr;
    }

    Connection getConnection() throws Exception {
        if (conn != null) return this.conn;

        String url = hiveURL;
        String driver = hiveClassName;

        Class.forName(driver);
        LOGGER.info("getting connection");

        conn = DriverManager.getConnection(url);

        LOGGER.info("got connection");
        return conn;
    }
}
