package org.openphc.cce.sla;

import org.junit.jupiter.api.Test;
import org.openphc.cce.common.intelligence.IntelligenceActionEvaluator;
import org.openphc.cce.common.kafka.IntelligenceTriggerProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Boots the real application context.
 *
 * <p>Every other test here constructs its subject directly, so nothing else exercises the actual
 * wiring. That leaves a class of failure invisible behind a high coverage figure: a bean this service
 * never names in source but needs at runtime. {@code @ComponentScan}, {@code @EntityScan} and
 * {@code @EnableJpaRepositories} are all widened to {@code org.openphc.cce}, so the context also
 * instantiates what cce-common-util contributes — including
 * {@code FhirExpressionEvaluator}, whose constructor needs JSONLogic on the classpath even though
 * this service declares no JSONLogic dependency of its own.
 *
 * <p>It also validates {@code SlaTransitionFetchRepository}: Spring Data parses every {@code @Query} at
 * bootstrap, so a typo in the fetch JPQL fails here rather than on the first poll in production. That
 * matters more for this service than for its siblings, because it runs {@code ddl-auto: validate}
 * against a schema two other services own — a mapping it gets wrong is a failure to start, not a
 * failure to serve.
 *
 * <p>H2 with Flyway disabled and the poller parked: the subject is bean wiring, not the schema (the
 * owning migrations cover that) and not {@code FOR UPDATE SKIP LOCKED}, whose semantics only PostgreSQL
 * reproduces.
 *
 * <p>It also guards that the context contains cce-common-util's intelligence and Kafka beans, which a
 * breach's intelligence actions are evaluated and published through. Building a {@code KafkaTemplate}
 * does not connect to a broker, so none is needed here.
 */
@SpringBootTest
@ActiveProfiles("contexttest")
class ApplicationContextTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void contextLoads() {
        // Fails on any wiring, mapping or missing-runtime-dependency problem.
    }

    @Test
    void intelligenceAndKafkaAreWired() {
        // What a breach's intelligence actions go through. All three come from cce-common-util and
        // Spring Boot's Kafka auto-configuration rather than this service's own code, so only booting the
        // context shows they are there.
        assertEquals(1, context.getBeanNamesForType(IntelligenceActionEvaluator.class).length);
        assertEquals(1, context.getBeanNamesForType(IntelligenceTriggerProducer.class).length);
        assertEquals(1, context.getBeanNamesForType(KafkaTemplate.class).length);
    }
}
