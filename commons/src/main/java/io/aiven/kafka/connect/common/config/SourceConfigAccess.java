package io.aiven.kafka.connect.common.config;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.config.validators.SourcenameTemplateValidator;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static io.aiven.kafka.connect.common.config.FileNameFragment.FILE_NAME_TEMPLATE_CONFIG;
import static io.aiven.kafka.connect.common.source.task.DistributionType.OBJECT_HASH;

public interface SourceConfigAccess extends AccessBase {

    default String getTargetTopic() {
        return getAbstractConfig().getString(Fragment.TARGET_TOPIC);
    }
    default String getSourceName() {
        return getAbstractConfig().getString(FILE_NAME_TEMPLATE_CONFIG);
    }

    default int getMaxPollRecords() {
        return getAbstractConfig().getInt(Fragment.MAX_POLL_RECORDS);
    }

    default int getExpectedMaxMessageBytes() {
        return getAbstractConfig().getInt(Fragment.EXPECTED_MAX_MESSAGE_BYTES);
    }

    default ErrorsTolerance getErrorsTolerance() {
        return ErrorsTolerance.forName(getAbstractConfig().getString(Fragment.ERRORS_TOLERANCE));
    }

    default DistributionType getDistributionType() {
        return DistributionType.forName(getAbstractConfig().getString(Fragment.DISTRIBUTION_TYPE));
    }

    /**
     * Gets the ring buffer size.
     *
     * @return the ring buffer size.
     */
    default int getRingBufferSize() {
        return getAbstractConfig().getInt(Fragment.RING_BUFFER_SIZE);
    }

    final class Fragment extends FragmentBase<SourceConfigAccess> {
        private static final String GROUP_OTHER = "OTHER_CFG";
        public static final String MAX_POLL_RECORDS = "max.poll.records";
        public static final String EXPECTED_MAX_MESSAGE_BYTES = "expected.max.message.bytes";
        public static final String TARGET_TOPIC = "topic";
        public static final String ERRORS_TOLERANCE = "errors.tolerance";

        public static final String DISTRIBUTION_TYPE = "distribution.type";
        /* public so that deprecated users can reference it */
        public static final String RING_BUFFER_SIZE = "ring.buffer.size";

        private Fragment() {
            super();
        }

        public static Setter setter(Map<String, String> config) {
            return new Setter(config);
        }

        public static ConfigDef update(final ConfigDef configDef) {

            // Offset Storage config group includes target topics
            int offsetStorageGroupCounter = 0;

            configDef.define(RING_BUFFER_SIZE, ConfigDef.Type.INT, 1000, ConfigDef.Range.atLeast(0),
                    ConfigDef.Importance.MEDIUM, "The number of storage key to store in the ring buffer.", GROUP_OTHER,
                    ++offsetStorageGroupCounter, ConfigDef.Width.SHORT, RING_BUFFER_SIZE);

            configDef.define(TARGET_TOPIC, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM, "eg : logging-topic", GROUP_OTHER, offsetStorageGroupCounter++,
                    ConfigDef.Width.NONE, TARGET_TOPIC);
            configDef.define(DISTRIBUTION_TYPE, ConfigDef.Type.STRING, OBJECT_HASH.name(),
                    new ObjectDistributionStrategyValidator(), ConfigDef.Importance.MEDIUM,
                    "Based on tasks.max config and the type of strategy selected, objects are processed in distributed"
                            + " way by Kafka connect workers, supported values : "
                            + Arrays.stream(DistributionType.values())
                                    .map(DistributionType::value)
                                    .collect(Collectors.joining(", ")),
                    GROUP_OTHER, ++offsetStorageGroupCounter, ConfigDef.Width.NONE, DISTRIBUTION_TYPE);

            int sourcePollingConfigCounter = 0;

            configDef.define(MAX_POLL_RECORDS, ConfigDef.Type.INT, 500, ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.MEDIUM, "Max poll records", GROUP_OTHER, ++sourcePollingConfigCounter,
                    ConfigDef.Width.NONE, MAX_POLL_RECORDS);
            // KIP-298 Error Handling in Connect
            configDef.define(ERRORS_TOLERANCE, ConfigDef.Type.STRING, ErrorsTolerance.NONE.name(),
                    new ErrorsToleranceValidator(), ConfigDef.Importance.MEDIUM,
                    "Indicates to the connector what level of exceptions are allowed before the connector stops, supported values : none,all",
                    GROUP_OTHER, ++sourcePollingConfigCounter, ConfigDef.Width.NONE, ERRORS_TOLERANCE);

            configDef.define(EXPECTED_MAX_MESSAGE_BYTES, ConfigDef.Type.INT, 1_048_588, ConfigDef.Importance.MEDIUM,
                    "The largest record batch size allowed by Kafka config max.message.bytes", GROUP_OTHER,
                    ++sourcePollingConfigCounter, ConfigDef.Width.NONE, EXPECTED_MAX_MESSAGE_BYTES);

            // step on earlier definition.
            configDef.configKeys().remove(FILE_NAME_TEMPLATE_CONFIG);
            configDef.define(FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM,
                    "The template for file names on S3. "
                            + "Supports `{{ variable }}` placeholders for substituting variables. "
                            + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                            + "(the offset of the first record in the file). "
                            + "Only some combinations of variables are valid, which currently are:\n"
                            + "- `topic`, `partition`, `start_offset`."
                            + "There is also `*` only available when using `hash` distribution.",
                    GROUP_OTHER, ++sourcePollingConfigCounter, ConfigDef.Width.LONG, FILE_NAME_TEMPLATE_CONFIG);


            return configDef;
        }

        public static void validate(SourceConfigAccess access) {
            new SourcenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG, access.getDistributionType())
                    .ensureValid(FILE_NAME_TEMPLATE_CONFIG, access.getSourceName());
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

            @Override
            public String toString() {
                return "Must be one of: "
                        + Arrays.stream(DistributionType.values()).map(DistributionType::name).collect(Collectors.toList());
            }
        }

        /**
         * The Fragment setter.
         */
        public static class Setter extends AbstractFragmentSetter<Setter> {
            /**
             * Constructor.
             *
             * @param data
             *            the data to modify.
             */
            private Setter(final Map<String, String> data) {
                super(data);
            }

            /**
             * Set the maximum poll records.
             *
             * @param maxPollRecords
             *            the maximum number of records to poll.
             * @return this
             */
            public Setter maxPollRecords(final int maxPollRecords) {
                return setValue(MAX_POLL_RECORDS, maxPollRecords);
            }

            /**
             * Sets the error tolerance.
             *
             * @param tolerance
             *            the error tolerance
             * @return this.
             */
            public Setter errorsTolerance(final ErrorsTolerance tolerance) {
                return setValue(ERRORS_TOLERANCE, tolerance.name());
            }

            /**
             * Sets the target topic.
             *
             * @param targetTopic
             *            the target topic.
             * @return this.
             */
            public Setter targetTopic(final String targetTopic) {
                return setValue(TARGET_TOPIC, targetTopic);
            }

            /**
             * Sets the distribution type.
             *
             * @param distributionType
             *            the distribution type.
             * @return this
             */
            public Setter distributionType(final DistributionType distributionType) {
                return setValue(DISTRIBUTION_TYPE, distributionType.name());
            }

            /**
             * Sets the ring buffer size.
             *
             * @param ringBufferSize
             *            the ring buffer size
             * @return this.
             */
            public Setter ringBufferSize(final int ringBufferSize) {
                return setValue(RING_BUFFER_SIZE, ringBufferSize);
            }
        }
    }
}
