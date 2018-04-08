# Migration from EclipseLink to Hibernate

## Overview
In this document, we'll go through steps necessary to migrate applications from using EclipseLink JPA to using Hibernate JPA.The migration will not need to convert any EclipseLink annotations to Hibernate annotations in application code. 

## Migration main steps
- [add hibernate dependency](#1)
- [remove EclipseLink](#2)
- [configure properties](#3)

<h2 id = "1"></h2>
### Add hibernate dependency
By default, Spring Data uses Hibernate as the default JPA implementation provider.So we just add **spring-boot-starter-data-jpa** dependency.If you have already added it, skip this step.

    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>

<h2 id = "2"></h2>
### Remove EclipseLink dependency
If you don't want to remove EclipseLink,you can skip this step.

- remove EclipseLink dependency 

        <dependency>
            <groupId>org.eclipse.persistence</groupId>
            <artifactId>org.eclipse.persistence.jpa</artifactId>
            <version>${eclipselink.version}</version>
        </dependency>

- remove EclipseLink static weaving

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

- remove EclipseLinkJpaConfig class

remove EclipseLinkJpaConfig class in org.apache.griffin.core.config package.  

<h2 id = "3"></h2>
#### Configure properties
You need to configure properties according to the database you use in Griffin.

Please see [Mysql and PostgreSQL switch in EclipseLink](https://github.com/apache/incubator-griffin/tree/master/griffin-doc/service/mysql_postgresql_switch_in_eclipselink.md) to configure.