# Mysql and postgresql switch

## Overview
By default, Griffin uses EclipseLink as the default JPA implementation. This document provides ways to switch mysql and postgresql.

- [Use mysql database](#1.1)
- [Use postgresql database](#1.2)

<h2 id = "1.1"></h2>

## Use mysql database 
### Add mysql dependency

    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
    </dependency>
### Configure properties

- configure application.properties

        spring.datasource.url = jdbc:mysql://localhost:3306/quartz?autoReconnect=true&useSSL=false
        spring.datasource.username = griffin
        spring.datasource.password = 123456
        spring.jpa.generate-ddl=true
        spring.datasource.driver-class-name = com.mysql.jdbc.Driver
        spring.jpa.show-sql = true
   If you use hibernate as your jpa implentation, you need also to add following configuration.
     
        spring.jpa.hibernate.ddl-auto = update
        spring.jpa.hibernate.naming-strategy = org.hibernate.cfg.ImprovedNamingStrategy
- configure quartz.properties

      org.quartz.jobStore.driverDelegateClass=org.quartz.impl.jdbcjobstore.StdJDBCDelegate

<h2 id = "1.2"></h2>

## Use postgresql database 

### Add postgresql dependency

    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
    </dependency>

### Configure properties
- configure application.properties

        spring.datasource.url = jdbc:postgresql://localhost:5432/quartz?autoReconnect=true&useSSL=false
        spring.datasource.username = griffin
        spring.datasource.password = 123456
        spring.jpa.generate-ddl=true
        spring.datasource.driver-class-name = org.postgresql.Driver
        spring.jpa.show-sql = true
  If you use hibernate as your jpa implentation, you need also to add following configuration.
     
        spring.jpa.hibernate.ddl-auto = update
        spring.jpa.hibernate.naming-strategy = org.hibernate.cfg.ImprovedNamingStrategy
       
- configure quartz.properties

      org.quartz.jobStore.driverDelegateClass=org.quartz.impl.jdbcjobstore.PostgreSQLDelegate
      
