/*
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import io.aiven.kafka.connect.common.integration.KafkaIntegrationTestBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;

public abstract class AbstractIntegrationTest extends KafkaIntegrationTestBase {
    private static final Set<String> CONNECTOR_NAMES = new HashSet<>();

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();

    private KafkaManager kafkaManager;

    @BeforeEach
    void setupKafka() throws IOException, ExecutionException, InterruptedException {
        kafkaManager = setupKafka(true, AivenKafkaConnectS3SinkConnector.class);

        final var topicName = IntegrationBase.topicName(testInfo);
        kafkaManager.createTopic(topicName);
    }

    @AfterEach
    void tearDown() {
        CONNECTOR_NAMES.forEach(kafkaManager::deleteConnector);
        CONNECTOR_NAMES.clear();
    }

    protected void createConnector(final Map<String, String> connectorConfig) {
        CONNECTOR_NAMES.add(connectorConfig.get("name"));
        kafkaManager.configureConnector(connectorConfig.get("name"), connectorConfig);
    }
}
