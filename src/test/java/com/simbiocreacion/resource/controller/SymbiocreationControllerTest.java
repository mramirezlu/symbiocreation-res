package com.simbiocreacion.resource.controller;

import com.simbiocreacion.resource.model.Participant;
import com.simbiocreacion.resource.model.Symbiocreation;
import com.simbiocreacion.resource.model.User;
import com.simbiocreacion.resource.service.ILlmService;
import com.simbiocreacion.resource.service.ISymbiocreationService;
import com.simbiocreacion.resource.service.IUserService;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifica el punto 2 (batch de completeUsers): mismo resultado que antes,
 * pero con una sola consulta findAllById en vez de un findById por participante (N+1).
 */
class SymbiocreationControllerTest {

    // ---------- helpers ----------
    private static User user(String id) {
        User u = new User();
        u.setId(id);
        return u;
    }

    private static Participant participant(String uId) {
        Participant p = new Participant();
        p.setU_id(uId);
        return p;
    }

    private static Symbiocreation symbioWith(Participant... participants) {
        Symbiocreation s = new Symbiocreation();
        s.setParticipants(new ArrayList<>(Arrays.asList(participants)));
        return s;
    }

    // ---------- assignUsers: lógica de asignación pura ----------

    @Test
    void assignUsers_assignsEachParticipantItsUser() {
        Participant p1 = participant("u1");
        Participant p2 = participant("u2");
        Symbiocreation s = symbioWith(p1, p2);
        User u1 = user("u1");
        User u2 = user("u2");
        Map<String, User> map = new HashMap<>();
        map.put("u1", u1);
        map.put("u2", u2);

        SymbiocreationController.assignUsers(s, map);

        assertSame(u1, p1.getUser());
        assertSame(u2, p2.getUser());
    }

    @Test
    void assignUsers_duplicateUIdsShareTheSameUser() {
        Participant p1 = participant("u1");
        Participant p2 = participant("u1");
        Symbiocreation s = symbioWith(p1, p2);
        User u1 = user("u1");
        Map<String, User> map = new HashMap<>();
        map.put("u1", u1);

        SymbiocreationController.assignUsers(s, map);

        assertSame(u1, p1.getUser());
        assertSame(u1, p2.getUser());
    }

    @Test
    void assignUsers_missingUserOrNullUId_leavesNull() {
        Participant missing = participant("nope");
        Participant nullId = participant(null);
        Symbiocreation s = symbioWith(missing, nullId);
        Map<String, User> map = new HashMap<>();
        map.put("u1", user("u1"));

        SymbiocreationController.assignUsers(s, map);

        assertNull(missing.getUser());
        assertNull(nullId.getUser());
    }

    @Test
    void assignUsers_nullParticipants_doesNotThrow() {
        Symbiocreation s = new Symbiocreation();
        s.setParticipants(null);

        assertSame(s, SymbiocreationController.assignUsers(s, new HashMap<>()));
    }

    // ---------- completeUsers vía getMine: batch (1 consulta) + resultado ----------

    @Test
    void getMine_completesUsersWithASingleBatchQuery() {
        ISymbiocreationService symbioService = mock(ISymbiocreationService.class);
        IUserService userService = mock(IUserService.class);
        ILlmService llmService = mock(ILlmService.class);
        FluxProcessor<Symbiocreation, Symbiocreation> processor = DirectProcessor.create();
        SymbiocreationController controller =
                new SymbiocreationController(symbioService, userService, llmService, processor);

        // Simbio con u1, u2, u1 (duplicado) y un participante sin u_id
        Participant p1 = participant("u1");
        Participant p2 = participant("u2");
        Participant p3 = participant("u1");
        Participant p4 = participant(null);
        Symbiocreation s = symbioWith(p1, p2, p3, p4);

        User u1 = user("u1");
        User u2 = user("u2");

        when(symbioService.findAllByUser(eq("user1"), any(Pageable.class)))
                .thenReturn(Flux.just(s));
        when(userService.findAllById(any()))
                .thenReturn(Flux.just(u1, u2));

        StepVerifier.create(controller.findByUserId("user1", 0))
                .assertNext(out -> {
                    List<Participant> ps = out.getParticipants();
                    assertSame(u1, ps.get(0).getUser());
                    assertSame(u2, ps.get(1).getUser());
                    assertSame(u1, ps.get(2).getUser()); // duplicado obtiene el mismo user
                    assertNull(ps.get(3).getUser());       // u_id null queda sin user
                })
                .verifyComplete();

        // Batch: una sola consulta findAllById, con ids únicos y sin null; nunca findById (N+1)
        verify(userService, times(1)).findAllById(argThat(ids -> {
            List<String> list = new ArrayList<>();
            ids.forEach(list::add);
            return list.size() == 2 && list.contains("u1") && list.contains("u2");
        }));
        verify(userService, never()).findById(anyString());
    }

    // ---------- perfil público: getPublicOfUser usa la variante SOLO públicas ----------

    @Test
    void getPublicOfUser_usesPublicOnlyQueryAndCompletesUsers() {
        ISymbiocreationService symbioService = mock(ISymbiocreationService.class);
        IUserService userService = mock(IUserService.class);
        ILlmService llmService = mock(ILlmService.class);
        FluxProcessor<Symbiocreation, Symbiocreation> processor = DirectProcessor.create();
        SymbiocreationController controller =
                new SymbiocreationController(symbioService, userService, llmService, processor);

        Symbiocreation s = symbioWith(participant("u1"));
        User u1 = user("u1");
        when(symbioService.findPublicByUser(eq("user1"), any(Pageable.class))).thenReturn(Flux.just(s));
        when(userService.findAllById(any())).thenReturn(Flux.just(u1));

        StepVerifier.create(controller.findPublicOfUser("user1", 0))
                .assertNext(out -> assertSame(u1, out.getParticipants().get(0).getUser()))
                .verifyComplete();

        // Debe usar la consulta filtrada a públicas, NO findAllByUser (que traería también las privadas)
        verify(symbioService, times(1)).findPublicByUser(eq("user1"), any(Pageable.class));
        verify(symbioService, never()).findAllByUser(anyString(), any(Pageable.class));
    }
}
