/*
 * Copyright 2020 Aiven Oy
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

package aiven.kafka.connect.azure.source;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectRunner.class);

    private final File pluginDir;
    private final String bootstrapServers;
    private final int offsetFlushInterval;

    private Herder herder;
    private Connect connect;

    public ConnectRunner(final File pluginDir, final String bootstrapServers, final int offsetFlushIntervalMs) {
        this.pluginDir = pluginDir;
        this.bootstrapServers = bootstrapServers;
        this.offsetFlushInterval = offsetFlushIntervalMs;
    }

    void start() {
        final Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", bootstrapServers);

        workerProps.put("offset.flush.interval.ms", Integer.toString(offsetFlushInterval));

        // These don't matter much (each connector sets its own converters), but need to be filled with valid classes.
        workerProps.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter.schemas.enable", "false");

        // Don't need it since we'll memory MemoryOffsetBackingStore.
        workerProps.put("offset.storage.file.filename", "");

        workerProps.put("plugin.path", pluginDir.getPath());

        final Time time = Time.SYSTEM;
        final String workerId = "test-worker";
        final String kafkaClusterId = "test-cluster";

        final Plugins plugins = new Plugins(workerProps);
        final StandaloneConfig config = new StandaloneConfig(workerProps);
        final Worker worker = new Worker(workerId, time, plugins, config, new MemoryOffsetBackingStore());
        herder = new StandaloneHerder(worker, kafkaClusterId);

        final RestServer rest = new RestServer(config);

        connect = new Connect(herder, rest);
        connect.start();
    }

    void createConnector(final Map<String, String> config) throws ExecutionException, InterruptedException {
        assert herder != null;

        final FutureCallback<Herder.Created<ConnectorInfo>> callback = new FutureCallback<>(
                new Callback<Herder.Created<ConnectorInfo>>() {
                    @Override
                    public void onCompletion(final Throwable error, final Herder.Created<ConnectorInfo> info) {
                        if (error == null) {
                            LOG.info("Created connector {}", info.result().name());
                        } else {
                            LOG.error("Failed to create job");
                        }
                    }
                });
        herder.putConnectorConfig(config.get(ConnectorConfig.NAME_CONFIG), config, false, callback);

        final Herder.Created<ConnectorInfo> connectorInfoCreated = callback.get();
        assert connectorInfoCreated.created();
    }

    void stop() {
        connect.stop();
    }

    void awaitStop() {
        connect.awaitStop();
    }
}
