package com.simbiocreacion.resource.controller;

import com.simbiocreacion.resource.dto.IdeaAiResponse;
import com.simbiocreacion.resource.dto.IdeaRequest;
import com.simbiocreacion.resource.model.*;
import com.simbiocreacion.resource.service.ILlmService;
import com.simbiocreacion.resource.service.ISymbiocreationService;
import com.simbiocreacion.resource.service.IUserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/symbiocreations")
@Slf4j
public class SymbiocreationController {

    private final ISymbiocreationService symbioService;
    private final IUserService userService;
    private final ILlmService llmService;
    private final FluxProcessor<Symbiocreation, Symbiocreation> symbioProcessor;
    private final FluxSink symbioSink;

    public SymbiocreationController(ISymbiocreationService symbioService,
                                    IUserService userService,
                                    ILlmService llmService,
                                    FluxProcessor<Symbiocreation, Symbiocreation> symbioProcessor) {
        this.symbioService = symbioService;
        this.userService = userService;
        this.llmService = llmService;
        this.symbioProcessor = symbioProcessor;
        this.symbioSink = this.symbioProcessor.sink();
    }

    @PostMapping()
    public Mono<Symbiocreation> create(@RequestBody Symbiocreation s) {
        s.setLastModified(new Date());
        s.setCreationDateTime(new Date());
        // set id for creator node
        Node nodeCreator = new Node();
        nodeCreator.setId(UUID.randomUUID().toString());

        Participant p = s.getParticipants().get(0); // s has the participant
        nodeCreator.setU_id(p.getU_id());

        if (p.getUser().getName() != null) {
            nodeCreator.setName(p.getUser().getName());
        }
        if (p.getUser().getFirstName() != null && p.getUser().getLastName() != null) {
            nodeCreator.setName(p.getUser().getFirstName() + " " + p.getUser().getLastName());
        }

        s.getGraph().add(nodeCreator);
        return symbioService.create(s);
    }

    @GetMapping("/{id}")
    public Mono<Symbiocreation> findById(@PathVariable String id) {
        return symbioService.findById(id)
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas);
    }

    // Completa los datos de usuario de cada participante. Usa UNA sola consulta por lote (findAllById con $in)
    // en vez de una por participante (evita el N+1). El resultado es idéntico al enfoque anterior.
    private Mono<Symbiocreation> completeUsers(Symbiocreation s) {
        if (s.getParticipants() == null || s.getParticipants().isEmpty()) {
            return Mono.just(s);
        }

        List<String> ids = s.getParticipants().stream()
                .map(Participant::getU_id)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        if (ids.isEmpty()) {
            return Mono.just(s); // participantes sin u_id: user queda null, igual que antes
        }

        return this.userService.findAllById(ids)
                .collectMap(User::getId, u -> u)
                .map(usersById -> assignUsers(s, usersById));
    }

    // Asigna a cada participante su usuario del mapa (o null si no está). Extraído para poder testearlo.
    static Symbiocreation assignUsers(Symbiocreation s, Map<String, User> usersById) {
        if (s.getParticipants() != null) {
            for (Participant p : s.getParticipants()) {
                p.setUser(usersById.get(p.getU_id()));
            }
        }
        return s;
    }

    // completes users in comments of idea
    private Mono<Node> completeUsersInComments(Node n) {
        if (n.getIdea() == null || n.getIdea().getComments() == null || n.getIdea().getComments().isEmpty()) {
            return Mono.just(n);
        }

        Mono<Map<String, User>> monoOfMap = Flux.fromStream(n.getIdea().getComments().stream())
//                .flatMapIterable(comments -> comments)
                .flatMap(c -> this.userService.findById(c.getU_id()))
                .collectMap(
                        item -> item.getId(),
                        item -> item
                );

        return Mono.create(callback -> {
            monoOfMap.subscribe(
                    map -> {
                        for (Comment comment : n.getIdea().getComments()) {
                            comment.setAuthor(map.get(comment.getU_id()));
                        }
                        callback.success(n);
                    }
            );
        });
    }

    private Mono<Node> completeUser(Node n) {
        return Mono.create(callback -> {
            if (n.getU_id() != null) {
                this.userService.findById(n.getU_id()).subscribe(u -> {
                    if (u != null) {
                        n.setUser(u);
                    }
                    callback.success(n);
                });
            } else callback.success(n);
        });
    }

    private Mono<Comment> completeUser(Comment c) {
        return Mono.create(callback -> {
            this.userService.findById(c.getU_id()).subscribe(u -> {
                if (u != null) {
                    c.setAuthor(u);
                }
                callback.success(c);
            });
        });
    }

    // removes ideas from each node but leaves the title
    private Mono<Symbiocreation> removeIdeas(Symbiocreation s) {
        return Mono.create(callback -> {
            // set nodes's ideas to null for a light payload
            Stack<Node> stack = new Stack<>();
            s.getGraph().forEach(node -> stack.push(node));
            Node current;

            while(!stack.isEmpty()) {
                current = stack.pop();
                if (current.getIdea() != null) {
                    current.getIdea().setDescription(null);
                    current.getIdea().setLastModified(null);
                    current.getIdea().setImgPublicIds(null);
                    current.getIdea().setExternalUrls(null);
                    current.getIdea().setComments(null);
                }
                if (current.getChildren() != null) current.getChildren().forEach(child -> stack.push(child));
            }
            callback.success(s);
        });
    }

    // TODO: should not receive any param -> get user from session
    // can we get all the users from all symbios in a map, so as to not make repeated user lookups
    @GetMapping("/getMine/{userId}/{page}")
    public Flux<Symbiocreation> findByUserId(@PathVariable String userId, @PathVariable int page) {
        // Orden por fecha de creación descendente (del último al primero); el Pageable lo aplica en Mongo junto con skip/limit.
        Pageable paging = PageRequest.of(page, 12, Sort.by("creationDateTime").descending());
        return symbioService.findAllByUser(userId, paging)
                .flatMap(this::completeUsers); // users needed for displaying participants' names in grid or list view
    }

    // Perfil público: solo las simbios públicas del usuario (mismo orden y paginación que getMine).
    @GetMapping("/getPublicOfUser/{userId}/{page}")
    public Flux<Symbiocreation> findPublicOfUser(@PathVariable String userId, @PathVariable int page) {
        Pageable paging = PageRequest.of(page, 12, Sort.by("creationDateTime").descending());
        return symbioService.findPublicByUser(userId, paging)
                .flatMap(this::completeUsers);
    }

    @GetMapping("/countPublicOfUser/{userId}")
    public Mono<Long> countPublicOfUser(@PathVariable String userId) {
        return symbioService.countPublicByUser(userId);
    }

    // Explore: público filtrado por nombre (opcional) y rango de fecha de creación (from/to en epoch millis, opcionales),
    // ordenado por creationDateTime desc.
    @GetMapping("/getAllPublic/{page}")
    public Flux<Symbiocreation> findPublicAll(@PathVariable int page,
                                              @RequestParam(required = false) String name,
                                              @RequestParam(required = false) Long from,
                                              @RequestParam(required = false) Long to) {
        Pageable paging = PageRequest.of(page, 20, Sort.by("creationDateTime").descending());
        return symbioService.findPublicFiltered("public", name,
                        from != null ? new Date(from) : null,
                        to != null ? new Date(to) : null,
                        paging)
                .flatMap(this::completeUsers);
    }

    // Ranking de públicas para el frontpage (Destacados = ideas, Nuevos = fecha creación, Más colaborados = participantes).
    // flatMapSequential preserva el orden del ranking.
    @GetMapping("/getPublicRanked")
    public Flux<Symbiocreation> findPublicRanked(@RequestParam(required = false) String name,
                                                 @RequestParam(required = false, defaultValue = "new") String sort,
                                                 @RequestParam(required = false, defaultValue = "20") int limit,
                                                 @RequestParam(required = false, defaultValue = "0") int page,
                                                 @RequestParam(required = false) Long from,
                                                 @RequestParam(required = false) Long to) {
        return symbioService.getPublicRanked(name, sort, limit, page,
                        from != null ? new Date(from) : null,
                        to != null ? new Date(to) : null)
                .flatMapSequential(this::completeUsers);
    }

    @GetMapping("/getUpcomingPublic/{page}")
    public Flux<Symbiocreation> findPublicUpcoming(@PathVariable int page,
                                                    @RequestParam(required = false) String name) {
        Pageable paging = PageRequest.of(page, 20, Sort.by("dateTime").ascending());
        if (name != null && !name.isBlank()) {
            return symbioService.findByVisibilityAndDateTimeGreaterThanEqualAndNameContainingIgnoreCase("public", new Date(), name, paging)
                    .flatMap(this::completeUsers);
        }
        return symbioService.findByVisibilityAndDateTimeGreaterThanEqual("public", new Date(), paging)
                .flatMap(this::completeUsers);
    }

    @GetMapping("/getPastPublic/{page}")
    public Flux<Symbiocreation> findPublicPast(@PathVariable int page,
                                                @RequestParam(required = false) String name) {
        Pageable paging = PageRequest.of(page, 20, Sort.by("dateTime").descending());
        if (name != null && !name.isBlank()) {
            return symbioService.findByVisibilityAndDateTimeLessThanEqualAndNameContainingIgnoreCase("public", new Date(), name, paging)
                    .flatMap(this::completeUsers);
        }
        return symbioService.findByVisibilityAndDateTimeLessThanEqual("public", new Date(), paging)
                .flatMap(this::completeUsers);
    }

    @GetMapping("/countByUser/{userId}")
    public Mono<Long> countSymbiocreationsByUser(@PathVariable String userId) {
        return symbioService.countByUser(userId);
    }

    @GetMapping("/countPublic")
    public Mono<Long> countPublicSymbiocreations(@RequestParam(required = false) String name,
                                                 @RequestParam(required = false) Long from,
                                                 @RequestParam(required = false) Long to) {
        return symbioService.countPublicFiltered("public", name,
                from != null ? new Date(from) : null,
                to != null ? new Date(to) : null);
    }

    @GetMapping("/countPastPublic")
    public Mono<Long> countPastPublicSymbiocreations() {
        return symbioService.countByVisibilityAndDateTimeLessThanEqual("public", new Date());
    }

    @GetMapping("/countUpcomingPublic")
    public Mono<Long> countUpcomingPublicSymbiocreations() {
        return symbioService.countByVisibilityAndDateTimeGreaterThanEqual("public", new Date());
    }

    @PutMapping("/{id}/updateName")
    public Mono<Void> updateName(@PathVariable String id, @RequestBody Symbiocreation newSymbio) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    s.setName(newSymbio.getName());
                    s.setLastModified(new Date());
                    return this.symbioService.update(s);
                })
                .then();
    }

    @PutMapping("/{id}/updateInfo")
    public Mono<Void> updateInfo(@PathVariable String id, @RequestBody Symbiocreation newSymbio) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    s.setName(newSymbio.getName());
                    s.setPlace(newSymbio.getPlace());
                    s.setDateTime(newSymbio.getDateTime());
                    s.setTimeZone(newSymbio.getTimeZone());
                    s.setHasStartTime(newSymbio.getHasStartTime());

                    s.setDescription(newSymbio.getDescription());
                    s.setInfoUrl(newSymbio.getInfoUrl());
                    s.setTags(newSymbio.getTags());
                    s.setExtraUrls(newSymbio.getExtraUrls());
                    s.setSdgs(newSymbio.getSdgs());

                    //s.setEnabled();
                    s.setVisibility(newSymbio.getVisibility());
                    s.setLastModified(new Date());
                    return this.symbioService.update(s);
                })
                .then();
    }

    @DeleteMapping("/{id}")
    public Mono<Void> delete(@PathVariable String id) {
        return this.symbioService.delete(id);
    }

    // Operations with nodes
    @GetMapping("/{id}/getNode/{nodeId}")
    public Mono<Node> findNodeById(@PathVariable String id, @PathVariable String nodeId) {
        return this.symbioService.findById(id)
                .map(s -> traverseAndGetNode(s.getGraph(), nodeId))
                .flatMap(this::completeUser)
                .flatMap(this::completeUsersInComments);
    }

    @GetMapping("/{id}/getNodesByUserId/{userId}")
    public Flux<Node> findNodesByUserId(@PathVariable String id, @PathVariable String userId) {
        return this.symbioService.findById(id)
                .map(s -> traverseAndGetNodesByUserId(s.getGraph(), userId))
                .flatMapMany(Flux::fromIterable);
    }

    @PutMapping("/{id}/updateNodeIdea")
    public Mono<Node> updateNodeIdea(@PathVariable String id, @RequestBody Node newNode) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    this.recurseAndReplaceIdea(s.getGraph(), newNode);
                    s.setLastModified(new Date());
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .doOnNext(symbioSink::next)
                .thenReturn(newNode);
    }

    @GetMapping("/{id}/getIdeasFromAI")
    public Mono<List<IdeaAiResponse>> getIdeasFromAI(@PathVariable String id) {
        return this.symbioService.findById(id)
                .flatMap(this.llmService::getIdeasForSymbioFromLlm);
    }

    @GetMapping("/{id}/getIdeasFromAI/{nodeId}")
    public Mono<List<IdeaAiResponse>> getIdeasFromAI(@PathVariable String id, @PathVariable String nodeId) {
        return this.symbioService.findById(id)
                .flatMap(symbio ->
                        Mono.fromSupplier(() -> this.traverseAndGetNode(symbio.getGraph(), nodeId))
                                .flatMap(node -> this.llmService.getIdeasForGroupFromLlm(symbio, node)));
    }

    // "Busco inspiración": genera 3 ideas basadas solo en el tema de la sesión (name + description),
    // sin requerir ideas existentes. Sirve tanto para nodos hoja como para grupos.
    @GetMapping("/{id}/getInspirationFromAI")
    public Mono<List<IdeaAiResponse>> getInspirationFromAI(@PathVariable String id) {
        return this.symbioService.findById(id)
                .flatMap(this.llmService::getInspirationIdeasFromLlm);
    }

    // TODO [Manera recomendada]: Al actualizar Spring AI y revertir getImageFromLlm a Mono<Image>,
    //  cambiar este método para recibir Image y usar UrlResource con image.getUrl() como antes.
    @PostMapping("/getImageFromAI")
    @ResponseBody
    public Mono<ResponseEntity<byte[]>> getImagesFromAI(@RequestBody IdeaRequest idea) {

        return this.llmService.getImageFromLlm(idea)
                .map(imageBytes -> ResponseEntity.ok()
                        .contentType(MediaType.IMAGE_PNG)
                        .body(imageBytes))
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping(value = "/getImageFromAi2", produces = MediaType.IMAGE_PNG_VALUE)
    @ResponseBody
    public Mono<Resource> getImagesFromAI2() {

        URI uri = URI.create("https://oaidalleapiprodscus.blob.core.windows.net/private/org-ATo7lO6xk7dzzRKpJyZd30Za/symbio-service-account/img-QvGM2632tTrhC2n4MryHvgf8.png?st=2024-09-27T06%3A01%3A13Z&se=2024-09-27T08%3A01%3A13Z&sp=r&sv=2024-08-04&sr=b&rscd=inline&rsct=image/png&skoid=d505667d-d6c1-4a0a-bac7-5c84a87759f8&sktid=a48cca56-e6da-484e-a814-9c849652bcb3&skt=2024-09-26T23%3A20%3A46Z&ske=2024-09-27T23%3A20%3A46Z&sks=b&skv=2024-08-04&sig=iRK/ygIdf3pfaaO4Q4kCqM4JKtNxhDNrnmKg2RAhoPY%3D");
        Resource resource = null;
        try {
            resource = new UrlResource(uri);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        return Mono.just(resource);
    }

    @PutMapping("/{id}/createCommentOfIdea/{nodeId}")
    public Mono<Comment> createCommentOfIdea(@PathVariable String id, @PathVariable String nodeId, @RequestBody Comment comment) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    comment.setLastModified(new Date());
                    this.recurseAndAddComment(s.getGraph(), nodeId, comment);
                    return this.symbioService.update(s);
                })
                .flatMap(s -> this.completeUser(comment));
    }

    @PostMapping("/{id}/createParticipant")
    public Mono<Symbiocreation> createParticipant(@PathVariable String id, @RequestBody Participant p) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    // add new participant and node objects
                    s.getParticipants().add(p);

                    Node newNode = new Node();
                    newNode.setId(UUID.randomUUID().toString());
                    newNode.setU_id(p.getU_id());

                    if (p.getUser().getName() != null) newNode.setName(p.getUser().getName());
                    if (p.getUser().getFirstName() != null && p.getUser().getLastName() != null) newNode.setName(p.getUser().getFirstName() + " " + p.getUser().getLastName());

                    s.getGraph().add(newNode);
                    s.setLastModified(new Date());
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    @PostMapping("/{id}/createUserNode")
    public Mono<Symbiocreation> createUserNode(@PathVariable String id, @RequestBody Node n) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    n.setId(UUID.randomUUID().toString());
                    s.getGraph().add(n);
                    s.setLastModified(new Date());
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    // creates a parent node for node with id childId
    @PostMapping("/{id}/createNextLevelGroup/{childId}")
    public Mono<Symbiocreation> createNextLevelGroup(@PathVariable String id, @PathVariable String childId, @RequestBody Node nextLevelNode) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    nextLevelNode.setId(UUID.randomUUID().toString());
                    // get child node
                    Node child = this.traverseAndGetNode(s.getGraph(), childId);
                    // set child as 'ambassador' if child is a user node.
                    if (child.getU_id() != null) {
                        child.setRole("ambassador");
                    }
                    // remove child node from graph
                    this.recurseAndDeleteNode(s.getGraph(), childId);
                    // add child node to parent
                    nextLevelNode.getChildren().add(child);
                    s.getGraph().add(nextLevelNode);
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    @GetMapping("/{id}/setParentNode/{childId}/{parentId}")
    public Mono<Symbiocreation> setParentNode(@PathVariable String id, @PathVariable String childId, @PathVariable String parentId) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    int n1 = this.traverseAndCount(s.getGraph());

                    Node child = this.traverseAndGetNode(s.getGraph(), childId);
                    child.setRole("participant");
                    // remove child node from graph
                    this.recurseAndDeleteNode(s.getGraph(), childId);
                    // add child node to parent
                    if (parentId.equals("none")) s.getGraph().add(child);
                    else this.recurseAndAddNodeAsChild(s.getGraph(), child, parentId);

                    int n2 = this.traverseAndCount(s.getGraph());
                    // safe-check (just in case!)
                    // node count of pre and post graphs should match
                    if (n1 == n2) {
                        s.setLastModified(new Date());
                        return this.symbioService.update(s);
                    } else {
                        return Mono.empty();
                    }
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    @DeleteMapping("/{id}/deleteNode/{nodeId}")
    public Mono<Symbiocreation> deleteNode(@PathVariable String id, @PathVariable String nodeId) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    Node toDelete = this.traverseAndGetNode(s.getGraph(), nodeId);
                    this.recurseAndDeleteNode(s.getGraph(), nodeId);
                    // if group node
                    if (toDelete.getChildren() != null) {
                        s.getGraph().addAll(toDelete.getChildren());
                        // all ambassador members lose their roles
                        toDelete.getChildren().forEach(child -> child.setRole("participant"));
                    }
                    s.setLastModified(new Date());
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    @PutMapping("/{id}/updateNodeName")
    public Mono<Symbiocreation> updateNodeName(@PathVariable String id, @RequestBody Node newNode) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    this.recurseAndReplaceName(s.getGraph(), newNode);
                    s.setLastModified(new Date());
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    // set participant as moderator
    @PutMapping("/{id}/setParticipantAsModerator")
    public Mono<Symbiocreation> setParticipantAsModerator(@PathVariable String id, @RequestBody Participant participant) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    for (int i = 0; i < s.getParticipants().size(); i++) {
                        if (s.getParticipants().get(i).getU_id().equals(participant.getU_id())) {
                            s.getParticipants().get(i).setIsModerator(true);
                            break;
                        }
                    }
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    // node contains id and new role
    @PutMapping("/{id}/updateUserNodeRole")
    public Mono<Symbiocreation> updateUserNodeRole(@PathVariable String id, @RequestBody Node node) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    this.recurseAndReplaceRole(s.getGraph(), node);
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    // participant has the new value for isModerator
    @PutMapping("/{id}/setParticipantIsModerator")
    public Mono<Symbiocreation> updateParticipantIsModerator(@PathVariable String id, @RequestBody Participant participant) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    for (int i = 0; i < s.getParticipants().size(); i++) {
                        if (s.getParticipants().get(i).getU_id().equals(participant.getU_id())) {
                            s.getParticipants().get(i).setIsModerator(participant.getIsModerator());
                        }
                    }
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    @DeleteMapping("/{id}/deleteParticipant/{u_id}")
    public Mono<Symbiocreation> deleteParticipant(@PathVariable String id, @PathVariable String u_id) {
        return this.symbioService.findById(id)
                .flatMap(s -> {
                    // remove participant
                    s.getParticipants().removeIf(p -> p.getU_id().equals(u_id));
                    // remove all nodes associated
                    this.recurseAndDeleteNodesOfUser(s.getGraph(), u_id);
                    return this.symbioService.update(s);
                })
                .flatMap(this::completeUsers)
                .flatMap(this::removeIdeas)
                .doOnNext(symbioSink::next);
    }

    @GetMapping("/{symbioId}/export-participants-data")
    @ResponseBody
    public ResponseEntity<Mono<Resource>> downloadParticipantsData(@PathVariable String symbioId) {
        Mono fetchedContent = symbioService.findById(symbioId)
                .flatMapIterable(s -> s.getParticipants())
                .flatMap(p -> this.userService.findById(p.getU_id()))
                .collect(Collectors.toList())
                .flatMap(users -> this.symbioService.generateParticipantsDataCsv(users))
                .map(InputStreamResource::new);

        String fileName = "participants-data-" + symbioId + ".csv";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,  "attachment; filename=" + fileName)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
                .body(fetchedContent);
    }

    @GetMapping("/{symbioId}/export-all-data")
    @ResponseBody
    public ResponseEntity<Mono<Resource>> downloadAllData(@PathVariable String symbioId) {
        Mono fetchedContent = symbioService.findById(symbioId)
                .flatMap(this::completeUsers)
                .flatMap(this.symbioService::generateAllDataCsv)
                .map(InputStreamResource::new);

        String fileName = "all-data-" + symbioId + ".csv";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,  "attachment; filename=" + fileName)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
                .body(fetchedContent);
    }


    /****************** Graph traversal helpers **********************/

    private void recurseAndReplaceIdea(List<Node> nodes, Node newNode) {
        nodes.forEach(n -> {
            if (n.getId().equals(newNode.getId())) {
                n.setIdea(newNode.getIdea());
                return;
            }
            if (n.getChildren() != null) this.recurseAndReplaceIdea(n.getChildren(), newNode);
        });
    }

    private void recurseAndDeleteNode(List<Node> nodes, String nodeId) {
        nodes.forEach(n -> {
            if (n.getChildren() != null) this.recurseAndDeleteNode(n.getChildren(), nodeId);
        });
        nodes.removeIf(node -> node.getId().equals(nodeId));
    }

    private void recurseAndDeleteNodesOfUser(List<Node> nodes, String userId) {
        nodes.forEach(n -> {
            if (n.getChildren() != null) this.recurseAndDeleteNodesOfUser(n.getChildren(), userId);
        });
        nodes.removeIf(node -> node.getU_id() != null && node.getU_id().equals(userId));
    }

    private void recurseAndAddNodeAsChild(List<Node> nodes, Node child, String parentId) {
        nodes.forEach(n -> {
            if (n.getId().equals(parentId)) {
                n.getChildren().add(child);
                return;
            }
            if (n.getChildren() != null) this.recurseAndAddNodeAsChild(n.getChildren(), child, parentId);
        });
    }

    private void recurseAndReplaceName(List<Node> nodes, Node newNode) {
        nodes.forEach(n -> {
            if (n.getId().equals(newNode.getId())) {
                n.setName(newNode.getName());
                return;
            }
            if (n.getChildren() != null) this.recurseAndReplaceName(n.getChildren(), newNode);
        });
    }

    private void recurseAndReplaceRole(List<Node> nodes, Node newNode) {
        nodes.forEach(n -> {
            if (n.getId().equals(newNode.getId())) {
                n.setRole(newNode.getRole());
                return;
            }
            if (n.getChildren() != null) this.recurseAndReplaceRole(n.getChildren(), newNode);
        });
    }

    private void recurseAndAddComment(List<Node> nodes, String nodeId, Comment comment) {
        nodes.forEach(node -> {
            if (node.getId().equals(nodeId)) {
                if (node.getIdea().getComments() == null) node.getIdea().setComments(new ArrayList<>());
                node.getIdea().getComments().add(comment);
                return;
            }
            if (node.getChildren() != null) this.recurseAndAddComment(node.getChildren(), nodeId, comment);
        });
    }

    private int traverseAndCount(List<Node> nodes) {
        int counter = 0;
        Stack<Node> stack = new Stack<Node>();
        nodes.forEach(node -> stack.push(node));
        Node current;

        while (!stack.isEmpty()) {
            current = stack.pop();
            counter++;

            if (current.getChildren() != null) current.getChildren().forEach(child -> stack.push(child));
        }
        return counter;
    }

    private Node traverseAndGetNode(List<Node> nodes, String nodeId) {
        Node n = null;
        Stack<Node> stack = new Stack<>();
        nodes.forEach(stack::push);
        Node current;

        while (!stack.isEmpty()) {
            current = stack.pop();
            if (current.getId().equals(nodeId)) {
                n = current;
                break;
            }
            if (current.getChildren() != null) current.getChildren().forEach(child -> stack.push(child));
        }
        return n;
    }

    private List<Node> traverseAndGetNodesByUserId(List<Node> nodes, String userId) {
        List<Node> result = new ArrayList<>();
        Stack<Node> stack = new Stack<>();
        nodes.forEach(stack::push);
        Node current;

        while (!stack.isEmpty()) {
            current = stack.pop();
            if (current.getU_id() != null && current.getU_id().equals(userId)) {
                result.add(current);
            }
            if (current.getChildren() != null) current.getChildren().forEach(stack::push);
        }
        return result;
    }

    /*
    @GetMapping("/symbiocreations/remodel")
    public void remodel() {
        symbioService.findAll()
                .flatMap(s -> {
                    //this.recurse(s.getGraph()
                    s.getParticipants().forEach(p -> {
                        List<Node> nodes = this.traverseAndGetNodesByUserId(s.getGraph(), p.getU_id());
                        for (Node n : nodes) {
                            if (p.getRole() != null && p.getRole().equals("ambassador")) {
                                n.setRole("ambassador");
                            } else {
                                n.setRole("participant");
                            }
                        }
                    });
                    return symbioService.update(s);
                }).subscribe();
    }*/

}
