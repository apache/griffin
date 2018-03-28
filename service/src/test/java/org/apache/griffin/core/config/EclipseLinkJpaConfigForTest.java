package org.apache.griffin.core.config;

import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.orm.jpa.JpaBaseConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.JpaProperties;
import org.springframework.boot.autoconfigure.transaction.TransactionManagerCustomizers;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.orm.jpa.vendor.AbstractJpaVendorAdapter;
import org.springframework.orm.jpa.vendor.EclipseLinkJpaVendorAdapter;
import org.springframework.transaction.jta.JtaTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@TestConfiguration
@ComponentScan("org.apache.griffin.core")
public class EclipseLinkJpaConfigForTest extends JpaBaseConfiguration {
    protected EclipseLinkJpaConfigForTest(DataSource ds, JpaProperties properties,
                                          ObjectProvider<JtaTransactionManager> jtm,
                                          ObjectProvider<TransactionManagerCustomizers> tmc) {
        super(ds, properties, jtm, tmc);
    }

    @Override
    protected AbstractJpaVendorAdapter createJpaVendorAdapter() {
        return new EclipseLinkJpaVendorAdapter();
    }

    @Override
    protected Map<String, Object> getVendorProperties() {
        Map<String, Object> map = new HashMap<>();
        map.put(PersistenceUnitProperties.WEAVING, "false");
        map.put(PersistenceUnitProperties.DDL_GENERATION, "create-or-extend-tables");
        return map;
    }
}
