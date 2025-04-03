/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.common.config;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static io.aiven.kafka.connect.common.source.task.DistributionType.OBJECT_HASH;

public final class SourceConfigFragment extends ConfigFragment {
    private static final String GROUP_OTHER = "OTHER_CFG";
    public static final String MAX_POLL_RECORDS = "max.poll.records";
    public static final String EXPECTED_MAX_MESSAGE_BYTES = "expected.max.message.bytes";
    public static final String TARGET_TOPIC = "topic";
    public static final String ERRORS_TOLERANCE = "errors.tolerance";
    public static final String DISTRIBUTION_TYPE = "distribution.type";
    public static final String RING_BUFFER_SIZE = "ring.buffer.size";

    public static Setter setter(Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Construct the ConfigFragment..
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    public SourceConfigFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    public static ConfigDef update(final ConfigDef configDef) {
        int sourcePollingConfigCounter = 0;

        configDef.define(RING_BUFFER_SIZE, ConfigDef.Type.INT, 1000, ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.MEDIUM, "The number of storage key to store in the ring buffer.", GROUP_OTHER, sourcePollingConfigCounter++,
                ConfigDef.Width.SHORT, RING_BUFFER_SIZE);

        configDef.define(MAX_POLL_RECORDS, ConfigDef.Type.INT, 500, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Max poll records", GROUP_OTHER, sourcePollingConfigCounter++,
                ConfigDef.Width.SHORT, MAX_POLL_RECORDS);
        // KIP-298 Error Handling in Connect
        configDef.define(ERRORS_TOLERANCE, ConfigDef.Type.STRING, ErrorsTolerance.NONE.name(),
                new ErrorsToleranceValidator(), ConfigDef.Importance.MEDIUM,
                "Indicates to the connector what level of exceptions are allowed before the connector stops, supported values : none,all",
                GROUP_OTHER, sourcePollingConfigCounter++, ConfigDef.Width.SHORT, ERRORS_TOLERANCE);

        configDef.define(EXPECTED_MAX_MESSAGE_BYTES, ConfigDef.Type.INT, 1_048_588, ConfigDef.Importance.MEDIUM,
                "The largest record batch size allowed by Kafka config max.message.bytes", GROUP_OTHER,
                sourcePollingConfigCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.SHORT, EXPECTED_MAX_MESSAGE_BYTES);

        // Offset Storage config group includes target topics
        int offsetStorageGroupCounter = 0;
        configDef.define(TARGET_TOPIC, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : logging-topic", GROUP_OTHER, offsetStorageGroupCounter++,
                ConfigDef.Width.SHORT, TARGET_TOPIC);
        configDef.define(DISTRIBUTION_TYPE, ConfigDef.Type.STRING, OBJECT_HASH.name(),
                new ObjectDistributionStrategyValidator(), ConfigDef.Importance.MEDIUM,
                "Based on tasks.max config and the type of strategy selected, objects are processed in distributed"
                        + " way by Kafka connect workers, supported values : "
                        + Arrays.stream(DistributionType.values())
                                .map(DistributionType::value)
                                .collect(Collectors.joining(", ")),
                GROUP_OTHER, offsetStorageGroupCounter++, ConfigDef.Width.SHORT, DISTRIBUTION_TYPE); // NOPMD
                                                                                                    // UnusedAssignment

        return configDef;
    }

    public String getTargetTopic() {
        return cfg.getString(TARGET_TOPIC);
    }

    public int getMaxPollRecords() {
        return cfg.getInt(MAX_POLL_RECORDS);
    }

    public int getExpectedMaxMessageBytes() {
        return cfg.getInt(EXPECTED_MAX_MESSAGE_BYTES);
    }

    public ErrorsTolerance getErrorsTolerance() {
        return ErrorsTolerance.forName(cfg.getString(ERRORS_TOLERANCE));
    }

    public DistributionType getDistributionType() {
        return DistributionType.forName(cfg.getString(DISTRIBUTION_TYPE));
    }

    public int getRingBufferSize() {
        return cfg.getInt(RING_BUFFER_SIZE);
    }

    private static class ErrorsToleranceValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String errorsTolerance = (String) value;
            if (StringUtils.isNotBlank(errorsTolerance)) {
                // This will throw an Exception if not a valid value.
                ErrorsTolerance.forName(errorsTolerance);
            }
        }
    }

    private static class ObjectDistributionStrategyValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String objectDistributionStrategy = (String) value;
            if (StringUtils.isNotBlank(objectDistributionStrategy)) {
                // This will throw an Exception if not a valid value.
                DistributionType.forName(objectDistributionStrategy);
            }
        }
    }

    public static class Setter extends AbstractFragmentSetter<Setter> {

        protected Setter(Map<String, String> data) {
            super(data);
        }

        public Setter maxPollRecords(final int maxPollRecords) {
            return setValue(MAX_POLL_RECORDS, maxPollRecords);
        }

        public Setter expectedMaxMessageBytes(final int expectedMaxMessageBytes) {
            return setValue(EXPECTED_MAX_MESSAGE_BYTES, expectedMaxMessageBytes);
        }

        public Setter errorsTolerance(final String errorsTolerance) {
            return setValue(ERRORS_TOLERANCE, errorsTolerance);
        }

        public Setter errorsTolerance(ErrorsTolerance tolerance) {
            return setValue(ERRORS_TOLERANCE, tolerance.name());
        }

        public Setter targetTopic(final String targetTopic) {
            return setValue(TARGET_TOPIC, targetTopic);
        }

        public Setter distributionType(final String distributionType) {
            return setValue(DISTRIBUTION_TYPE, distributionType);
        }

        public Setter distributionType(DistributionType distributionType) {
            return setValue(DISTRIBUTION_TYPE, distributionType.name());
        }

        public Setter ringBufferSize(final int ringBufferSize) {
            return setValue(RING_BUFFER_SIZE, ringBufferSize);
        }
    }
}
