package com.simbiocreacion.resource.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simbiocreacion.resource.dto.IdeaAiResponse;
import com.simbiocreacion.resource.dto.IdeaRequest;
import com.simbiocreacion.resource.dto.TrendAiResponse;
import com.simbiocreacion.resource.model.Idea;
import com.simbiocreacion.resource.model.Node;
import com.simbiocreacion.resource.model.Symbiocreation;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.chat.prompt.PromptTemplate;
import org.springframework.ai.converter.BeanOutputConverter;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Service
public class LlmService implements ILlmService {

    private final ChatClient chatClient;

    // TODO [Manera recomendada]: Al actualizar Spring AI a una versión que soporte gpt-image-1,
    //  reemplazar este WebClient por ImageModel de Spring AI:
    //  private final ImageModel imageModel;
    private final WebClient openAiWebClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("classpath:/prompts/system-for-symbio-ideas.md")
    private Resource systemForSymbioIdeas;

    @Value("classpath:/prompts/system-for-group-ideas.md")
    private Resource systemForGroupIdeas;

    @Value("classpath:/prompts/user-for-symbio-trends.st")
    private Resource userForSymbioTrends;

    // Brief de dirección de arte para la generación de imágenes (lenguaje visual SIMBIO).
    @Value("classpath:/prompts/system-for-image.md")
    private Resource systemForImage;

    // Contenido del brief cacheado una sola vez (ver loadImagePrompt).
    private String imageArtDirection;

    private static final String USER_PROBLEM_TEMPLATE_1 = """
            This is the topic you need to address:
            {symbiocreationName}
            {symbiocreationDescription}
            
            """;

    private static final String USER_PROBLEM_TEMPLATE_2 = """
            This is the session's topic:
            {symbiocreationName}
            {symbiocreationDescription}
            
            """;
    private static final String USER_IDEA_TEMPLATE = """
            Título: %s
            Descripción: %s

            """;
    private static final String USER_QUERY_TEMPLATE_1 = """
            Give me three new ideas for the topic given to you.
            {format}
            """;

    private static final String USER_QUERY_INSPIRATION_TEMPLATE = """
            Give me three new ideas for the topic given to you.
            Write every idea entirely in Spanish: both the title and the description must be in Spanish,
            even if the topic is written in another language.
            {format}
            """;

    private static final String USER_QUERY_TEMPLATE_2 = """
            Give me one or more new ideas that consolidate the ideas given to you.
            {format}
            """;

    private static final Predicate<Idea> TITLE_OR_DESCRIPTION_EXISTS_PREDICATE = idea ->
            (Objects.nonNull(idea.getTitle()) && !idea.getTitle().trim().isEmpty())
                    || (Objects.nonNull(idea.getDescription()) && !idea.getDescription().trim().isEmpty());

    // Mensajes de respuesta cuando no hay ideas suficientes
    private static final String NO_IDEAS_TITLE = "Se necesitan más ideas";
    private static final String NO_IDEAS_FOR_SYMBIO_MSG = "Para usar esta funcionalidad, los participantes deben agregar al menos una idea a la sesión. Una vez que haya ideas disponibles, la IA podrá generar nuevas sugerencias basadas en ellas.";
    private static final String NO_IDEAS_FOR_GROUP_MSG = "Para consolidar ideas del grupo, los participantes deben agregar al menos una idea. Una vez que haya ideas disponibles, la IA podrá generar sugerencias consolidadas.";

    // Mensaje de respuesta cuando no hay tema (name/description) para generar inspiración
    private static final String NO_TOPIC_TITLE = "Falta un tema";
    private static final String NO_TOPIC_MSG = "Agrega un nombre o una descripción a la sesión para generar ideas de inspiración.";

    // Directiva para la generación de imágenes: evitar que el modelo dibuje texto
    // (gpt-image-1 tiende a renderizar texto, a menudo ilegible o con errores).
    private static final String IMAGE_NO_TEXT_DIRECTIVE =
            "Do not include any text, letters, words, numbers, labels, captions, typography, or watermarks anywhere in the image. "
            + "Produce a clean, purely visual illustration with no written characters of any kind.";

    // TODO [Manera recomendada]: Al actualizar Spring AI, cambiar la firma del constructor a:
    //  public LlmService(ChatClient.Builder builder, ImageModel imageModel)
    //  y eliminar el WebClient y apiKey.
    public LlmService(ChatClient.Builder builder,
                      @Value("${spring.ai.openai.api-key}") String openAiApiKey) {
        this.chatClient = builder.build();
        this.openAiWebClient = WebClient.builder()
                .baseUrl("https://api.openai.com/v1")
                .defaultHeader("Authorization", "Bearer " + openAiApiKey)
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10 MB para imágenes base64
                .build();
    }

    @PostConstruct
    private void loadImagePrompt() {
        try {
            this.imageArtDirection = systemForImage.getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("No se pudo cargar el prompt de dirección de arte de imágenes (system-for-image.md)", e);
        }
    }

    @Override
    public Mono<List<IdeaAiResponse>> getIdeasForSymbioFromLlm(Symbiocreation symbiocreation) {
        log.debug("Getting ideas from LLM for symbiocreation: {}", symbiocreation.getId());
        List<Idea> existingIdeas = getIdeasFromSymbiocreation(symbiocreation, 3);
        log.debug("Found {} existing ideas in symbiocreation", existingIdeas.size());

        if (existingIdeas.isEmpty()) {
            log.info("No existing ideas found for symbiocreation: {}, returning default message", symbiocreation.getId());
            return Mono.just(List.of(new IdeaAiResponse(NO_IDEAS_TITLE, NO_IDEAS_FOR_SYMBIO_MSG, true)));
        }

        return Mono.fromCallable(() -> {
            PromptTemplate userProblemPromptTemplate = new PromptTemplate(
                    USER_PROBLEM_TEMPLATE_1,
                    Map.of("symbiocreationName", sanitizeInput(symbiocreation.getName()),
                            "symbiocreationDescription", sanitizeInput(symbiocreation.getDescription())));
            Message userProblem = userProblemPromptTemplate.createMessage();

            StringBuilder sbUserIdeas = new StringBuilder();
            sbUserIdeas.append("These are some ideas from participants: \n\n");

            existingIdeas.forEach(idea -> {
                String ideaStr = String.format(
                        USER_IDEA_TEMPLATE,
                        Objects.toString(idea.getTitle(), ""),
                        Objects.toString(idea.getDescription(), ""));
                sbUserIdeas.append(ideaStr);
            });
            Message userIdeas = new UserMessage(sbUserIdeas.toString());

            BeanOutputConverter<List<IdeaAiResponse>> outputConverter = new BeanOutputConverter<>(
                    new ParameterizedTypeReference<List<IdeaAiResponse>>() { });
            PromptTemplate userQueryTemplate = new PromptTemplate(
                    USER_QUERY_TEMPLATE_1,
                    Map.of("format", outputConverter.getFormat()));
            Message userQuery = userQueryTemplate.createMessage();

            Prompt prompt = new Prompt(List.of(
                    new SystemMessage(systemForSymbioIdeas), userProblem, userIdeas, userQuery));
            log.debug("Sending prompt to LLM for symbiocreation: {}", symbiocreation.getId());
            String content = chatClient.prompt(prompt).call().content();
            log.debug("Received response from LLM: {}", content);
            List<IdeaAiResponse> llmResponse = outputConverter.convert(content);
            log.debug("Parsed {} ideas from LLM response", llmResponse != null ? llmResponse.size() : 0);

            return validateResponse(llmResponse);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            log.error("Error getting ideas from LLM for symbiocreation: {}. Error type: {}. Message: {}",
                    symbiocreation.getId(), e.getClass().getSimpleName(), e.getMessage(), e);
            return Mono.just(Collections.emptyList());
        });
    }

    @Override
    public Mono<List<IdeaAiResponse>> getInspirationIdeasFromLlm(Symbiocreation symbiocreation) {
        log.debug("Getting inspiration ideas from LLM for symbiocreation: {}", symbiocreation.getId());

        String name = symbiocreation.getName();
        String description = symbiocreation.getDescription();
        boolean hasTopic = (name != null && !name.trim().isEmpty())
                || (description != null && !description.trim().isEmpty());

        if (!hasTopic) {
            log.info("No topic (name/description) for symbiocreation: {}, cannot generate inspiration", symbiocreation.getId());
            return Mono.just(List.of(new IdeaAiResponse(NO_TOPIC_TITLE, NO_TOPIC_MSG, true)));
        }

        return Mono.fromCallable(() -> {
            PromptTemplate userProblemPromptTemplate = new PromptTemplate(
                    USER_PROBLEM_TEMPLATE_1,
                    Map.of("symbiocreationName", sanitizeInput(name),
                            "symbiocreationDescription", sanitizeInput(description)));
            Message userProblem = userProblemPromptTemplate.createMessage();

            BeanOutputConverter<List<IdeaAiResponse>> outputConverter = new BeanOutputConverter<>(
                    new ParameterizedTypeReference<List<IdeaAiResponse>>() { });
            PromptTemplate userQueryTemplate = new PromptTemplate(
                    USER_QUERY_INSPIRATION_TEMPLATE,
                    Map.of("format", outputConverter.getFormat()));
            Message userQuery = userQueryTemplate.createMessage();

            // Sin bloque de "ideas de participantes": la inspiración se basa solo en el tema.
            Prompt prompt = new Prompt(List.of(
                    new SystemMessage(systemForSymbioIdeas), userProblem, userQuery));
            log.debug("Sending inspiration prompt to LLM for symbiocreation: {}", symbiocreation.getId());
            String content = chatClient.prompt(prompt).call().content();
            log.debug("Received inspiration response from LLM: {}", content);
            List<IdeaAiResponse> llmResponse = outputConverter.convert(content);

            return validateResponse(llmResponse);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            log.error("Error getting inspiration ideas from LLM for symbiocreation: {}. Error type: {}. Message: {}",
                    symbiocreation.getId(), e.getClass().getSimpleName(), e.getMessage(), e);
            return Mono.just(Collections.emptyList());
        });
    }

    @Override
    public Mono<List<IdeaAiResponse>> getIdeasForGroupFromLlm(Symbiocreation symbiocreation, Node group) {
        log.debug("Getting ideas from LLM for group: {} in symbiocreation: {}", group.getId(), symbiocreation.getId());
        List<Idea> groupIdeas = group.getChildren().stream()
                .map(Node::getIdea)
                .filter(Objects::nonNull)
                .filter(TITLE_OR_DESCRIPTION_EXISTS_PREDICATE)
                .toList();
        log.debug("Found {} ideas in group", groupIdeas.size());

        if (groupIdeas.isEmpty()) {
            log.info("No ideas found in group: {}, returning default message", group.getId());
            return Mono.just(List.of(new IdeaAiResponse(NO_IDEAS_TITLE, NO_IDEAS_FOR_GROUP_MSG, true)));
        }

        return Mono.fromCallable(() -> {
            PromptTemplate userSessionPromptTemplate = new PromptTemplate(
                    USER_PROBLEM_TEMPLATE_2,
                    Map.of("symbiocreationName", sanitizeInput(symbiocreation.getName()),
                            "symbiocreationDescription", sanitizeInput(symbiocreation.getDescription())));
            Message userSession = userSessionPromptTemplate.createMessage();

            StringBuilder sbUserIdeas = new StringBuilder();
            sbUserIdeas.append("These are the ideas you need to summarize: \n\n");

            groupIdeas.forEach(idea -> {
                String ideaStr = String.format(
                        USER_IDEA_TEMPLATE,
                        Objects.toString(idea.getTitle(), ""),
                        Objects.toString(idea.getDescription(), ""));
                sbUserIdeas.append(ideaStr);
            });
            Message userIdeas = new UserMessage(sbUserIdeas.toString());

            BeanOutputConverter<List<IdeaAiResponse>> outputConverter = new BeanOutputConverter<>(
                    new ParameterizedTypeReference<List<IdeaAiResponse>>() { });
            PromptTemplate userQueryTemplate = new PromptTemplate(
                    USER_QUERY_TEMPLATE_2,
                    Map.of("format", outputConverter.getFormat()));
            Message userQuery = userQueryTemplate.createMessage();

            Prompt prompt = new Prompt(List.of(
                    new SystemMessage(systemForGroupIdeas), userSession, userIdeas, userQuery));
            log.debug("Sending prompt to LLM for group: {}", group.getId());
            String content = chatClient.prompt(prompt).call().content();
            log.debug("Received response from LLM: {}", content);
            List<IdeaAiResponse> llmResponse = outputConverter.convert(content);
            log.debug("Parsed {} ideas from LLM response", llmResponse != null ? llmResponse.size() : 0);

            return validateResponse(llmResponse);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            log.error("Error getting ideas from LLM for group: {} in symbiocreation: {}. Error type: {}. Message: {}",
                    group.getId(), symbiocreation.getId(), e.getClass().getSimpleName(), e.getMessage(), e);
            return Mono.just(Collections.emptyList());
        });
    }

    // TODO [Manera recomendada]: Al actualizar Spring AI a una versión que soporte gpt-image-1,
    //  reemplazar todo este método por el uso de ImageModel de Spring AI:
    //
    //  @Override
    //  public Mono<Image> getImageFromLlm(IdeaRequest idea) {
    //      return Mono.fromCallable(() -> {
    //          ImagePrompt imagePrompt = new ImagePrompt(
    //              new ImageMessage(sanitizeInput(idea.title()) + "\n" + sanitizeInput(idea.description())),
    //              OpenAiImageOptions.builder()
    //                  .withModel("gpt-image-1")
    //                  .withQuality("high")
    //                  .withN(1)
    //                  .withHeight(1024)
    //                  .withWidth(1024)
    //                  .build());
    //          return imageModel.call(imagePrompt).getResult().getOutput();
    //      })
    //      .subscribeOn(Schedulers.boundedElastic())
    //      .onErrorResume(e -> {
    //          log.error("Error generating image from LLM for idea: {}", idea.title(), e);
    //          return Mono.empty();
    //      });
    //  }
    @Override
    public Mono<byte[]> getImageFromLlm(IdeaRequest idea) {
        String sanitizedTitle = sanitizeInput(idea.title());
        String sanitizedDescription = sanitizeInput(idea.description());
        // Brief de dirección de arte (marco) + la idea (protagonista) + restricción técnica de no-texto (al final).
        String prompt = imageArtDirection + "\n\n"
                + sanitizedTitle + "\n" + sanitizedDescription + "\n\n"
                + IMAGE_NO_TEXT_DIRECTIVE;

        Map<String, Object> requestBody = Map.of(
                "model", "gpt-image-1",
                "prompt", prompt,
                "n", 1,
                "size", "1024x1024",
                "quality", "medium"
        );

        return openAiWebClient.post()
                .uri("/images/generations")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(String.class)
                .map(responseBody -> {
                    try {
                        JsonNode root = objectMapper.readTree(responseBody);
                        String b64Data = root.path("data").get(0).path("b64_json").asText();
                        return Base64.getDecoder().decode(b64Data);
                    } catch (Exception e) {
                        throw new RuntimeException("Error parsing image response from OpenAI", e);
                    }
                })
                .doOnError(e -> log.error("Error generating image from OpenAI for idea: {}", idea.title(), e))
                .onErrorMap(this::mapImageError);
    }

    // I2: traduce el error de OpenAI a un estado HTTP que el frontend pueda distinguir.
    //  503: sin cuota · 422: contenido bloqueado por políticas · 502: fallo genérico.
    private Throwable mapImageError(Throwable e) {
        if (e instanceof WebClientResponseException wcre) {
            int status = wcre.getStatusCode().value();
            String code = "";
            String type = "";
            String message = "";
            try {
                JsonNode error = objectMapper.readTree(wcre.getResponseBodyAsString()).path("error");
                code = error.path("code").asText("");
                type = error.path("type").asText("");
                message = error.path("message").asText("");
            } catch (Exception ignore) {
                // cuerpo de error no parseable: se tratará como fallo genérico
            }

            if (status == 429 || "insufficient_quota".equals(code)) {
                return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "insufficient_quota", e);
            }
            String combined = (code + " " + type + " " + message).toLowerCase();
            if (combined.contains("content_policy") || combined.contains("moderation") || combined.contains("safety")) {
                return new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "content_policy", e);
            }
        }
        return new ResponseStatusException(HttpStatus.BAD_GATEWAY, "generation_failed", e);
    }

    @Override
    public Mono<List<TrendAiResponse>> getTrendsForSymbioFromLlm(Symbiocreation symbiocreation) {
        return Mono.fromCallable(() -> {
            StringBuilder sbIdeas = new StringBuilder();

            List<Idea> ideas = SymbiocreationService.getAllIdeasInSymbiocreation(symbiocreation).stream()
                    .filter(TITLE_OR_DESCRIPTION_EXISTS_PREDICATE)
                    .toList();
            ideas.forEach(idea -> {
                String ideaStr = String.format(
                        USER_IDEA_TEMPLATE,
                        Objects.toString(idea.getTitle(), ""),
                        Objects.toString(idea.getDescription(), ""));
                sbIdeas.append(ideaStr);
            });

            BeanOutputConverter<List<TrendAiResponse>> outputConverter = new BeanOutputConverter<>(
                    new ParameterizedTypeReference<List<TrendAiResponse>>() { });
            PromptTemplate userTrendsPromptTemplate = new PromptTemplate(
                    userForSymbioTrends,
                    Map.of("ideas", sbIdeas,
                            "format", outputConverter.getFormat()));
            Message userTrends = userTrendsPromptTemplate.createMessage();

            Prompt prompt = new Prompt(userTrends);
            String content = chatClient.prompt(prompt).call().content();
            List<TrendAiResponse> llmResponse = outputConverter.convert(content);

            return validateResponse(llmResponse);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            log.error("Error getting trends from LLM for symbiocreation: {}", symbiocreation.getId(), e);
            return Mono.just(Collections.emptyList());
        });
    }

    private List<Idea> getIdeasFromSymbiocreation(Symbiocreation symbiocreation, int maxIdeas) {
        List<Idea> ideas = SymbiocreationService.getAllIdeasInSymbiocreation(symbiocreation).stream()
                .filter(TITLE_OR_DESCRIPTION_EXISTS_PREDICATE)
                .collect(Collectors.toList());
        Collections.shuffle(ideas);
        return ideas.subList(0, Math.min(ideas.size(), maxIdeas));
    }

    private String sanitizeInput(String input) {
        if (input == null) {
            return "";
        }
        // Remove potential prompt injection patterns
        return input
                .replaceAll("(?i)ignore\\s+(all\\s+)?(previous|above|prior)\\s+(instructions?|prompts?)", "")
                .replaceAll("(?i)disregard\\s+(all\\s+)?(previous|above|prior)\\s+(instructions?|prompts?)", "")
                .replaceAll("(?i)forget\\s+(all\\s+)?(previous|above|prior)\\s+(instructions?|prompts?)", "")
                .trim();
    }

    private <T> List<T> validateResponse(List<T> response) {
        if (response == null) {
            return Collections.emptyList();
        }
        return response;
    }
}
