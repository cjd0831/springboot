package springboot.hbase.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

/**
 * Created by jingdong on 2018-06-26
 **/
@Configuration
public class SwaggerConfig {
	@Bean
	public Docket createRestApi() {
		return new Docket(DocumentationType.SWAGGER_2)
						.apiInfo(apiInfo())
						.select()
						.apis(RequestHandlerSelectors.basePackage("springboot.hbase.controller"))
						.paths(PathSelectors.any())
						.build();
	}

	private ApiInfo apiInfo() {
		return new ApiInfoBuilder()
						.title("Demo project for Spring Boot integration of HBase")
						.version("1.0")
						.build();
	}
}
