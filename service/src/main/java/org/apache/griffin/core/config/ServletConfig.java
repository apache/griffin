package org.apache.griffin.core.config;

import java.util.concurrent.TimeUnit;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @CLassName ServletConfig
 * @Description Jetty Config
 * @Author goodman
 * @Date 2019-02-18 15:48
 * @Version 1.0
 **/
@Configuration
public class ServletConfig {
    /**
     * Jetty Port
     **/
    private static final int JETTY_PORT = 8080;

    /**
     * Jetty Session Time out
     */
    private static final int SESSION_TIMEOUT = 1;


    @Bean
    public EmbeddedServletContainerFactory servletContainer() {
        JettyEmbeddedServletContainerFactory factory = new JettyEmbeddedServletContainerFactory();
        factory.setPort(JETTY_PORT);
        factory.setSessionTimeout(SESSION_TIMEOUT, TimeUnit.HOURS);
        return factory;
    }
}
