package com.simbiocreacion.resource.service;

import com.simbiocreacion.resource.model.Node;
import com.simbiocreacion.resource.model.User;
import com.simbiocreacion.resource.repository.SymbiocreationRepository;
import com.simbiocreacion.resource.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

@Service
@RequiredArgsConstructor
@Log4j2
public class UserService implements IUserService {

    private final UserRepository userRepository;

    private final SymbiocreationRepository symbiocreationRepository;

    @Override
    public Mono<User> create(User e) {
        e.setCreationDateTime(new Date());
        return userRepository.save(e);
    }

    @Override
    public Mono<User> findById(String id) {
        return userRepository.findById(id);
    }

    @Override
    public Flux<User> findAllById(Iterable<String> ids) {
        return userRepository.findAllById(ids);
    }

    @Override
    public Flux<User> findByEmail(String email) {
        return userRepository.findByEmail(email);
    }

    @Override
    public Flux<User> findAll() {
        return userRepository.findAll();
    }

    @Override
    public Mono<User> update(User e) {
        return userRepository.save(e);
    }

    @Override
    public Mono<Void> delete(String id) {
        return userRepository.deleteById(id);
    }

    @Override
    public Mono<Void> deleteAll() {
        return userRepository.deleteAll();
    }

    @Override
    public Mono<Long> count() {
        return userRepository.count();
    }

    @Override
    public Flux<Document> groupAndCountByDate() {
        return userRepository.groupAndCountByDate();
    }

    @Override
    public Flux<User> getTopUsersAdmin() {
        return userRepository.findTop10ByOrderByScoreDesc();
    }

    @Override
    public Flux<User> getTopUsersRankingPublic() {
        return userRepository.findTop100ByOrderByScoreDesc();
    }

    @Override
    public Mono<User> recomputeScore(String userId) {

        return userRepository.findById(userId)
                .flatMap(this::computeScoreGeneral);
    }

    private Mono<User> computeScoreGeneral(User user) {
        final int coefParticipant = 1;
        final int coefAmbassador = 2;

        Mono<Integer> userScoreMono = this.symbiocreationRepository.findAllByUserWithGraphs(user.getId())
                .parallel()
                .map(s -> {
                    final Set<Node> roots = SymbiocreationService.findTreeRoots(s);
                    final Map<Node, Node> leafToRootMap = new HashMap<>();

                    return roots.stream()
                            .parallel()
                            .flatMap(root ->
                                    SymbiocreationService.findLeavesOf(root, new HashSet<>()).stream()
                                            .filter(leaf -> leaf.getU_id() != null &&
                                                    leaf.getU_id().equals(user.getId()))
                                            .map(leaf -> {
                                                leafToRootMap.put(leaf, root);
                                                return leaf;
                                            })
                            )
                            .mapToInt(leaf ->
                                    leaf.getRole() != null && leaf.getRole().equals("ambassador") ?
                                    coefAmbassador * SymbiocreationService.computeDepthOf(leaf, leafToRootMap.get(leaf)) :
                                    coefParticipant * SymbiocreationService.computeDepthOf(leaf, leafToRootMap.get(leaf))
                            )
                            .sum();
                })
                .reduce(Integer::sum)
                .defaultIfEmpty(0);

        return Mono.create(callback ->
                userScoreMono.subscribe(score -> {
                    user.setScore(score);

                    callback.success(user);
                })
        );
    }
}
