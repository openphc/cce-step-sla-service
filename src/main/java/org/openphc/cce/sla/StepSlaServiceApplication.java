package org.openphc.cce.sla;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * CCE Step SLA Service — the time plane: the SLA transition evaluator.
 *
 * <p>Owns everything driven by <em>time passing</em>: it picks up the
 * {@code step_sla_state_transition} rows the Matcher Service scheduled, advances
 * {@code step_instance.sla_status}, records the resulting {@code OVERDUE} / {@code MISSED}
 * deviations, and publishes the intelligence actions those deviations fire.
 *
 * <p>It does not match inbound events, enrol patients, create steps or manage definitions. Deviations
 * driven by an inbound event — {@code ORDER_VIOLATION} — stay with the Matcher Service, which detects
 * them at completion. The two services never write the same column.
 *
 * <p>{@code @EnableScheduling} drives {@link org.openphc.cce.sla.service.SlaTransitionEvaluator}.
 * Component scanning is widened to {@code org.openphc.cce} so the beans cce-common-util contributes are
 * found alongside this service's own — among them {@code IntelligenceActionEvaluator} and the
 * {@code IntelligenceTriggerProducer} it publishes through, backed by Spring Boot's Kafka
 * auto-configuration (producer only; this service consumes nothing).
 */
@SpringBootApplication(scanBasePackages = "org.openphc.cce")
// @Entity and @Repository types are not picked up by component scanning, so both are pointed at
// org.openphc.cce as well: the shared entities and repositories live in cce-common-util.
@EntityScan("org.openphc.cce")
@EnableJpaRepositories("org.openphc.cce")
@EnableScheduling
public class StepSlaServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(StepSlaServiceApplication.class, args);
    }
}
