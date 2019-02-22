package org.apache.griffin.core.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @CLassName SwaggerConfig
 * @Description Swagger-UI
 * @Author goodman
 * @Date 2019-02-18 16:02
 * @Version 1.0
 **/
@Configuration
@EnableSwagger2
public class SwaggerConfig {
    /**
     * Swagger Title
     **/
    private static final String SWAGGER_TITLE = "Griffin-DataPipeLine";
    /**
     * Swagger Description
     **/
    private static final String SWAGGER_DESCRIPTION = "apache griffin-dataPipeline-custom";
    /**
     * Swagger Doc Version
     **/
    private static final String DOC_VERSION = "apache griffin-dataPipeline-custom";

    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfo()).select()
                .apis(RequestHandlerSelectors.basePackage("org.apache.griffin.core"))
                .paths(PathSelectors.any())
                .build();
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title(SWAGGER_TITLE)
                .description(SWAGGER_DESCRIPTION)
                .version(DOC_VERSION)
                .build();
    }
}
