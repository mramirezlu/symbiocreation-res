# symbiocreation-res

Backend **reactivo** de Symbiocreation (co-creación / ideación colaborativa en tiempo real). Spring Boot 3 + WebFlux + MongoDB reactivo + RSocket, con funciones de IA (OpenAI / Spring AI).

## Comandos
./mvnw spring-boot:run   # levantar el servicio (puerto 8080)
./mvnw clean package     # compilar + empaquetar
./mvnw test              # correr tests

Requiere MongoDB en localhost:27017 (BD symbio_db) y una API key de OpenAI.

## Prompts de IA

Los prompts de LlmService están en src/main/resources/prompts/ (system-for-symbio-ideas.md, system-for-group-ideas.md, system-for-image.md, user-for-symbio-trends.st). Se cargan del classpath vía @Value; además hay plantillas inline en LlmService.
