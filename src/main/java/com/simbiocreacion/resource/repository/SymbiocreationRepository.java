package com.simbiocreacion.resource.repository;

import com.simbiocreacion.resource.model.Symbiocreation;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Date;

public interface SymbiocreationRepository extends ReactiveMongoRepository<Symbiocreation, String>, SymbiocreationRepositoryCustom {

    // El orden lo aporta el Pageable (Sort.by("creationDateTime").descending()) desde el controller.
    @Query(value = "{'participants.u_id': ?0}", fields = "{'graph': 0}")
    Flux<Symbiocreation> findAllByUser(String userId, Pageable pageable);

    // Perfil público: solo las simbios públicas en las que participa el usuario (orden por el Pageable).
    @Query(value = "{'participants.u_id': ?0, 'visibility': 'public'}", fields = "{'graph': 0}")
    Flux<Symbiocreation> findPublicByUser(String userId, Pageable pageable);

    @Query(value = "{'participants.u_id': ?0, 'visibility': 'public'}", count = true)
    Mono<Long> countPublicByUser(String userId);

    @Query(value = "{'participants.u_id': ?0}")
    Flux<Symbiocreation> findAllByUserWithGraphs(String userId);

    //@Query(value = "{'visibility': ?0}", fields = "{'graph': 0}", sort = "{lastModified: -1}")
    //Flux<Symbiocreation> findAllByVisibility(String visibility);

    // El listado público del Explore (nombre + rango de fecha de creación) se resuelve dinámicamente
    // en SymbiocreationRepositoryImpl.findPublicFiltered / countPublicFiltered.

    //@Query(value = "{'visibility': ?0, 'dateTime' : {'$gt' : ?1}}", fields = "{'graph': 0}", sort = "{dateTime: 1}")
    //Flux<Symbiocreation> findUpcomingByVisibility(String visibility, Date now);

    // Pageable object contains page index, page size, and sorting parameters
    @Query(fields = "{'graph': 0}")
    Flux<Symbiocreation> findByVisibilityAndDateTimeLessThanEqual(String visibility, Date now, Pageable pageable);

    @Query(value = "{'visibility': ?0, 'dateTime': {'$lte': ?1}, 'name': {$regex: ?2, $options: 'i'}}", fields = "{'graph': 0}")
    Flux<Symbiocreation> findByVisibilityAndDateTimeLessThanEqualAndNameContainingIgnoreCase(String visibility, Date now, String name, Pageable pageable);

    @Query(fields = "{'graph': 0}")
    Flux<Symbiocreation> findByVisibilityAndDateTimeGreaterThanEqual(String visibility, Date now, Pageable pageable);

    @Query(value = "{'visibility': ?0, 'dateTime': {'$gte': ?1}, 'name': {$regex: ?2, $options: 'i'}}", fields = "{'graph': 0}")
    Flux<Symbiocreation> findByVisibilityAndDateTimeGreaterThanEqualAndNameContainingIgnoreCase(String visibility, Date now, String name, Pageable pageable);

    Mono<Long> countByVisibility(String visibility);

    Mono<Long> countByVisibilityAndDateTimeLessThanEqual(String visibility, Date dateTime);

    Mono<Long> countByVisibilityAndDateTimeGreaterThanEqual(String visibility, Date dateTime);

    @Query(value = "{'participants.u_id': ?0}", count = true)
    Mono<Long> countByParticipantsU_id(String userId);
}
