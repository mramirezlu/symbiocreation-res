package com.simbiocreacion.resource.repository;

import com.simbiocreacion.resource.model.OneDot;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface OneDotRepository extends ReactiveMongoRepository<OneDot, String> {

    // Excluye 'screenshots' (historial pesado) pero conserva 'grid' para pintar la miniatura y el tamaño en el listado
    @Query(value = "{'participants.u_id': ?0}", fields = "{'screenshots': 0}", sort = "{lastModifiedAt: -1}") // ineffective to order in backend????
    Flux<OneDot> findAllByUser(String userId, Pageable pageable);

    @Query(value = "{'participants.u_id': ?0}", count = true)
    Mono<Long> countByParticipantsU_id(String userId);
}
