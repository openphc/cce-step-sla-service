package org.openphc.cce.sla.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Wires {@link MatcherLagGate}.
 *
 * <p>The Kafka admin client is built here from {@code cce.sla.matcher-lag-gate.*} rather than from
 * {@code spring.kafka.*}: Kafka auto-configuration is excluded on purpose (see
 * {@code StepSlaServiceApplication}), and this service needs no producer, consumer or template — only two
 * metadata reads. With the gate disabled, neither the client nor the reader exists, so Kafka is never
 * contacted.
 */
@Configuration
@EnableConfigurationProperties(MatcherLagGateProperties.class)
public class MatcherLagGateConfig {

    /** Prefix of the gate's settings in application.yml. */
    private static final String PROPERTY_PREFIX = "cce.sla.matcher-lag-gate";

    /** The on/off switch under {@link #PROPERTY_PREFIX}; the client and reader exist only when it is on. */
    private static final String ENABLED_PROPERTY = "enabled";

    /**
     * The Kafka admin client the reader uses. Created only when the gate is enabled; closed with the
     * context, which stops its network thread.
     *
     * @param gateProperties where the bootstrap servers, timeouts and any extra client settings come from
     * @return a client connected lazily, on its first call
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = PROPERTY_PREFIX, name = ENABLED_PROPERTY, matchIfMissing = true)
    public Admin matcherLagKafkaAdminClient(MatcherLagGateProperties gateProperties) {
        return Admin.create(buildKafkaAdminClientConfig(gateProperties));
    }

    /**
     * The reader that turns offsets into lag. Created only when the gate is enabled.
     *
     * @param matcherLagKafkaAdminClient the Kafka admin client above
     * @param gateProperties             which group and topic to read, and the time budget for one read
     * @return the reader the gate calls on each check
     */
    @Bean
    @ConditionalOnProperty(prefix = PROPERTY_PREFIX, name = ENABLED_PROPERTY, matchIfMissing = true)
    public MatcherLagReader matcherLagReader(Admin matcherLagKafkaAdminClient,
                                             MatcherLagGateProperties gateProperties) {
        return new MatcherLagReader(matcherLagKafkaAdminClient, gateProperties.groupId(), gateProperties.topic(),
                gateProperties.checkTimeout());
    }

    /**
     * The gate the evaluator asks before every batch. Always created, so the evaluator never needs to know
     * whether it is enabled; a disabled gate simply always says {@link MatcherLagGate.GateDecision#PROCESS}.
     *
     * @param gateProperties    thresholds, timeouts and switches
     * @param lagReaderProvider the reader when the gate is enabled; empty when it is not
     * @param meterRegistry     where the gate's gauges are registered
     * @return the gate
     */
    @Bean
    public MatcherLagGate matcherLagGate(MatcherLagGateProperties gateProperties,
                                         ObjectProvider<MatcherLagReader> lagReaderProvider,
                                         MeterRegistry meterRegistry) {
        return new MatcherLagGate(gateProperties, lagReaderProvider.getIfAvailable(), meterRegistry);
    }

    /**
     * Builds the Kafka admin client's settings.
     *
     * <p>The client's own timeouts are held to the check's budget, so a broker that never answers costs one
     * check-timeout rather than the client's 60s default on the single scheduler thread. Anything in
     * additional-kafka-admin-client-properties is applied last, so a deployment can still override these.
     *
     * @param gateProperties the gate's settings
     * @return the settings to create the Kafka admin client with
     */
    static Map<String, Object> buildKafkaAdminClientConfig(MatcherLagGateProperties gateProperties) {
        int checkTimeoutMillis = (int) gateProperties.checkTimeout().toMillis();
        Map<String, Object> kafkaAdminClientConfig = new HashMap<>();
        // Where the Matcher Service's cluster is.
        kafkaAdminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, gateProperties.bootstrapServers());
        // Names this client in broker logs and quotas, so its requests are recognisable.
        kafkaAdminClientConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, "cce-step-sla-service-matcher-lag-gate");
        // A safety cap on the Kafka admin client itself, not the timeout each call actually runs with.
        // MatcherLagReader gives every call only what is left of one shared check-timeout deadline — e.g.
        // describe topic gets 5s, and if it answers at 4s the next call gets 1s, not another 5s. These two
        // settings are the client's own upper limits underneath that, lowered from Kafka's defaults so it can
        // never hold a request open longer than one check, even if a future call forgets to pass a timeout:
        //   request.timeout.ms     — the longest one network request waits for the broker (Kafka default 30s)
        //   default.api.timeout.ms — the timeout for a call that is given none (Kafka default 60s)
        kafkaAdminClientConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, checkTimeoutMillis);
        kafkaAdminClientConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, checkTimeoutMillis);
        // Last, so a deployment's own settings (e.g. security.protocol) win over the defaults above.
        kafkaAdminClientConfig.putAll(gateProperties.additionalKafkaAdminClientProperties());
        return kafkaAdminClientConfig;
    }
}
