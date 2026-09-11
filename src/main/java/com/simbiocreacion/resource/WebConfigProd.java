package com.simbiocreacion.resource;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.reactive.config.CorsRegistry;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;

@Configuration
@EnableWebFlux
@Profile("prod")
public class WebConfigProd implements WebFluxConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry corsRegistry) {
        corsRegistry.addMapping("/**")
                .allowedOrigins("https://app-simbiocreacion.wydnex.com", "https://app.simbiocreacion.com", "https://symbiocreation-ui.vercel.app")
                .allowedMethods("*")
                .allowedHeaders("*")
                .maxAge(3600);
    }
}
