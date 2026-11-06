package org.openphc.cce.sla;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.openphc.cce.common.intelligence.IntelligenceActionEvaluator;
import org.openphc.cce.common.kafka.IntelligenceTriggerProducer;
import org.openphc.cce.sla.kafka.MatcherLagGate;
import org.openphc.cce.sla.kafka.MatcherLagReader;
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
 * <p>It also guards what the context must <em>not</em> contain: cce-common-util's intelligence and Kafka
 * beans, which the application class filters out of the scan.
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
    void intelligenceAndKafkaAreNotWired() {
        // Not part of this service yet. cce-common-util still ships these beans and spring-kafka still
        // reaches the classpath through it, so only the application class's scan filter and
        // auto-configuration exclusion keep them out. This is what notices if either is lost.
        assertEquals(0, context.getBeanNamesForType(IntelligenceActionEvaluator.class).length);
        assertEquals(0, context.getBeanNamesForType(IntelligenceTriggerProducer.class).length);
        assertEquals(0, context.getBeanNamesForType(KafkaTemplate.class).length);
    }

    /** The gate, its reader and its Kafka admin client are each created once from the gate's own settings. */
    @Test
    void matcherLagGateIsWiredWithItsAdminClient() {
        // The gate is this service's one Kafka client. It builds its own Kafka admin client rather than relying
        // on the excluded auto-configuration, so this is what notices if that wiring breaks.
        assertEquals(1, context.getBeanNamesForType(MatcherLagGate.class).length);
        assertEquals(1, context.getBeanNamesForType(MatcherLagReader.class).length);
        assertEquals(1, context.getBeanNamesForType(Admin.class).length);
    }
}
