package com.simbiocreacion.resource.service;

import com.simbiocreacion.resource.model.User;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface IUserService {

    Mono<User> create(User e);

    Mono<User> findById(String id);

    Flux<User> findAllById(Iterable<String> ids);

    Flux<User> findByEmail(String email);

    Flux<User> findAll();

    Mono<User> update(User e);

    Mono<Void> delete(String id);

    Mono<Void> deleteAll();

    Mono<Long> count();

    Flux<Document> groupAndCountByDate();

    Flux<User> getTopUsersAdmin();

    Flux<User> getTopUsersRankingPublic();

    Mono<User> recomputeScore(String userId);
}
