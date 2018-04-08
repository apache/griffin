# Migration from Hibernate to EclipseLink

## Overview
By default, Spring Data uses Hibernate as the default JPA implementation provider.However, Hibernate is certainly not the only JPA implementation available to us.In this document, we’ll go through steps necessary to set up EclipseLink as the implementation provider for Spring Data JPA.The migration will not need to convert any Hibernate annotations to EclipseLink annotations in application code. 

## Migration main steps
- [exclude hibernate dependencies](#1)
- [add EclipseLink dependency](#2)
- [configure EclipseLink static weaving](#3)
- [spring code configuration](#4)
- [configure properties](#5)

<h2 id = "1"></h2>

### Exclude hibernate dependencies
Since we want to use EclipseLink instead as the JPA provider, we don't need it anymore.Therefore we can remove it from our project by excluding its dependencies:

    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId>
        <exclusions>
            <exclusion>
                <groupId>org.hibernate</groupId>
                <artifactId>*</artifactId>
            </exclusion>
        </exclusions>
    </dependency>

<h2 id = "2"></h2>

### Add EclipseLink dependency
To use it in our Spring Boot application, we just need to add the org.eclipse.persistence.jpa dependency in the pom.xml of our project.

    <properties>
        <eclipselink.version>2.6.0</eclipselink.version>
    </properties>
    <dependency>
        <groupId>org.eclipse.persistence</groupId>
        <artifactId>org.eclipse.persistence.jpa</artifactId>
        <version>2.6.0</version>
    </dependency>
   
<h2 id = "3"></h2> 

### Configure EclipseLink static weaving
EclipseLink requires the domain types to be instrumented to implement lazy-loading. This can be achieved either through static weaving at compile time or dynamically at class loading time (load-time weaving). In Griffin,we use static weaving in pom.xml.

    <build>
        <plugins>
            <plugin>
                <groupId>com.ethlo.persistence.tools</groupId>
                <artifactId>eclipselink-maven-plugin</artifactId>
                <version>2.7.0</version>
                <executions>
                    <execution>
                        <phase>process-classes</phase>
                        <goals>
                            <goal>weave</goal>
                        </goals>
                    </execution>
                </executions>
                <dependencies>
                    <dependency>
                        <groupId>org.eclipse.persistence</groupId>
                        <artifactId>org.eclipse.persistence.jpa</artifactId>
                        <version>${eclipselink.version}</version>
                    </dependency>
                </dependencies>
            </plugin>
        </plugins>
    </build> 

<h2 id = "4"></h2>

### Spring configuration
**JpaBaseConfiguration is an abstract class which defines beans for JPA** in Spring Boot. Spring  provides a configuration implementation for Hibernate out of the box called HibernateJpaAutoConfiguration. However, for EclipseLink, we have to create a custom configuration.To customize it, we have to implement some methods like createJpaVendorAdapter() or getVendorProperties().
First, we need to implement the createJpaVendorAdapter() method which specifies the JPA implementation to use.
Also, we have to define some vendor-specific properties which will be used by EclipseLink.We can add these via the getVendorProperties() method.
**Add following code as a class to org.apache.griffin.core.config package in Griffin project.**
   

        @Configuration
        @ComponentScan("org.apache.griffin.core")
        public class EclipseLinkJpaConfig extends JpaBaseConfiguration {
            protected EclipseLinkJpaConfig(DataSource ds, JpaProperties properties,
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

<h2 id = "5"></h2>

#### Configure properties
You need to configure properties according to the database you use in Griffin.

Please see [Mysql and PostgreSQL switch in EclipseLink](https://github.com/apache/incubator-griffin/blob/master/griffin-doc/service/migration_from_eclipselink_to_hibernate.md) to configure.