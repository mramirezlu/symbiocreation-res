package com.simbiocreacion.resource.service;

import com.simbiocreacion.resource.model.Idea;
import com.simbiocreacion.resource.model.Symbiocreation;
import com.simbiocreacion.resource.model.User;
import org.bson.Document;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.util.Date;
import java.util.List;

public interface ISymbiocreationService {

    Mono<Symbiocreation> create(Symbiocreation e);

    Mono<Symbiocreation> findById(String id);

    Flux<Symbiocreation> findAll();

    Flux<Symbiocreation> findAllByUserId(String userId);

    Flux<Symbiocreation> findAllByUser(String userId, Pageable pageable);

    // Perfil público: solo simbios públicas del usuario + su conteo.
    Flux<Symbiocreation> findPublicByUser(String userId, Pageable pageable);

    Mono<Long> countPublicByUser(String userId);

    Flux<Symbiocreation> findPublicFiltered(String visibility, String name, Date from, Date to, Pageable pageable);

    Mono<Long> countPublicFiltered(String visibility, String name, Date from, Date to);

    // Ranking de simbios públicas para el frontpage. sort: "ideas" (cant. de ideas), "collaborators"
    // (cant. de participantes) o "new" (fecha de creación); siempre DESC. name filtra por nombre (opcional).
    // from/to filtran por rango de fecha de creación (opcionales). page/limit paginan el ranking (page base 0).
    Flux<Symbiocreation> getPublicRanked(String name, String sort, int limit, int page, Date from, Date to);

    Flux<Symbiocreation> findByVisibilityAndDateTimeLessThanEqual(String visibility, Date now, Pageable pageable);

    Flux<Symbiocreation> findByVisibilityAndDateTimeLessThanEqualAndNameContainingIgnoreCase(String visibility, Date now, String name, Pageable pageable);

    Flux<Symbiocreation> findByVisibilityAndDateTimeGreaterThanEqual(String visibility, Date now, Pageable pageable);

    Flux<Symbiocreation> findByVisibilityAndDateTimeGreaterThanEqualAndNameContainingIgnoreCase(String visibility, Date now, String name, Pageable pageable);

    Mono<Symbiocreation> update(Symbiocreation e);

    Mono<Void> delete(String id);

    Mono<Void> deleteAll();

    Mono<Long> count();

    Mono<Long> countByVisibility(String visibility);

    Mono<Long> countByVisibilityAndDateTimeLessThanEqual(String visibility, Date dateTime);

    Mono<Long> countByVisibilityAndDateTimeGreaterThanEqual(String visibility, Date dateTime);

    Mono<Long> countByUser(String userId);

    Mono<ByteArrayInputStream> generateParticipantsDataCsv(List<User> users);

    Mono<ByteArrayInputStream> generateAllDataCsv(Symbiocreation symbiocreation);

    Flux<Document> groupAndCountByDate();

    Mono<Long> countIdeasAll();

    Mono<Long> countIdeasAllOfUser(String userId);

    Mono<Long> countGroupsAsAmbassadorOfUser(String userId);

    Mono<Long> countIdeasAllOfSymbiocreation(String symbiocreationId);

    Flux<Idea> getIdeasAll();

    Flux<Idea> getIdeasAllOfSymbiocreation(String symbiocreationId);

    Flux<Idea> getIdeasAllVisibilityPublic();

    Flux<Document> getTopSymbiocreations();
}
