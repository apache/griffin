# Mysql and PostgreSQL switch in EclipseLink

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
    
- configure quartz.properties

      org.quartz.jobStore.driverDelegateClass=org.quartz.impl.jdbcjobstore.PostgreSQLDelegate
      
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
    spring.datasource.driver-class-name = com.mysql.jdbc.Driver
    spring.jpa.show-sql = true
    
- configure quartz.properties
 

     org.quartz.jobStore.driverDelegateClass=org.quartz.impl.jdbcjobstore.StdJDBCDelegate

