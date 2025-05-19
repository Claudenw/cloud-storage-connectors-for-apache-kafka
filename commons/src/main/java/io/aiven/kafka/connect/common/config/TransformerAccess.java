package io.aiven.kafka.connect.common.config;

import io.aiven.kafka.connect.common.source.input.InputFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Locale;
import java.util.Map;


public interface TransformerAccess extends AccessBase<TransformerAccess> {

    default InputFormat getInputFormat() {
        return InputFormat.valueOf(getAbstractConfig().getString(TransformerFragment.INPUT_FORMAT_KEY).toUpperCase(Locale.ROOT));
    }

    default String getSchemaRegistryUrl() {
        return getAbstractConfig().getString(TransformerFragment.SCHEMA_REGISTRY_URL);
    }

    default Class<?> getAvroValueSerializer() {
        return getAbstractConfig().getClass(TransformerFragment.AVRO_VALUE_SERIALIZER);
    }

    default int getTransformerMaxBufferSize() {
        return getAbstractConfig().getInt(TransformerFragment.TRANSFORMER_MAX_BUFFER_SIZE);
    }

    final class TransformerFragment extends FragmentBase<TransformerAccess>{
        private static final String TRANSFORMER_GROUP = "Transformer group";
        public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
        public static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL = "value.converter.schema.registry.url";
        public static final String AVRO_VALUE_SERIALIZER = "value.serializer";
        public static final String INPUT_FORMAT_KEY = "input.format";
        public static final String SCHEMAS_ENABLE = "schemas.enable";
        public static final String TRANSFORMER_MAX_BUFFER_SIZE = "transformer.max.buffer.size";
        private static final int DEFAULT_MAX_BUFFER_SIZE = 4096;

        private TransformerFragment() {
            super();
        }

        public static Setter setter(final Map<String, String> data) {
            return new Setter(data);
        }

        public static ConfigDef update(final ConfigDef configDef) {
            int transformerCounter = 0;
            configDef.define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", TRANSFORMER_GROUP, transformerCounter++,
                    ConfigDef.Width.NONE, SCHEMA_REGISTRY_URL);
            configDef.define(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", TRANSFORMER_GROUP,
                    transformerCounter++, ConfigDef.Width.NONE, VALUE_CONVERTER_SCHEMA_REGISTRY_URL);
            configDef.define(INPUT_FORMAT_KEY, ConfigDef.Type.STRING, InputFormat.BYTES.getValue(),
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                    "Input format of messages read from source avro/json/parquet/bytes", TRANSFORMER_GROUP,
                    transformerCounter++, ConfigDef.Width.NONE, INPUT_FORMAT_KEY);
            configDef.define(TRANSFORMER_MAX_BUFFER_SIZE, ConfigDef.Type.INT, DEFAULT_MAX_BUFFER_SIZE,
                    new ByteArrayTransformerMaxBufferSizeValidator(), ConfigDef.Importance.MEDIUM,
                    "Max Size of the byte buffer when using the BYTE Transformer", TRANSFORMER_GROUP, transformerCounter++,
                    ConfigDef.Width.NONE, TRANSFORMER_MAX_BUFFER_SIZE);
            configDef.define(AVRO_VALUE_SERIALIZER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM,
                    "Avro value serializer", TRANSFORMER_GROUP, transformerCounter++, // NOPMD
                    // UnusedAssignment
                    ConfigDef.Width.NONE, AVRO_VALUE_SERIALIZER);
            return configDef;
        }

        private static class ByteArrayTransformerMaxBufferSizeValidator implements ConfigDef.Validator {
            @Override
            public void ensureValid(final String name, final Object value) {

                // ConfigDef will throw an error if this is not an int that is supplied
                if ((int) value <= 0) {
                    throw new ConfigException(
                            String.format("%s must be larger then 0 and less then %s", name, Integer.MAX_VALUE));
                }

            }
        }

        /**
         * The setter for the TransformerFragment
         */
        public final static class Setter extends AbstractFragmentSetter<Setter> {
            /**
             * Constructor.
             *
             * @param data
             *            data to modify.
             */
            private Setter(final Map<String, String> data) {
                super(data);
            }

            /**
             * Sets the schema registry URL.
             *
             * @param schemaRegistryUrl
             *            the schema registry URL.
             * @return this
             */
            public Setter schemaRegistry(final String schemaRegistryUrl) {
                return setValue(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
            }

            /**
             * Sets the schema registry for the value converter schema registry URL.
             *
             * @param valueConverterSchemaRegistryUrl
             *            the schema registry URL.
             * @return this
             */
            public Setter valueConverterSchemaRegistry(final String valueConverterSchemaRegistryUrl) {
                return setValue(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, valueConverterSchemaRegistryUrl);
            }

            /**
             * Sets the input format.
             *
             * @param inputFormat
             *            the input format for the transformer.
             * @return this
             */
            public Setter inputFormat(final InputFormat inputFormat) {
                return setValue(INPUT_FORMAT_KEY, inputFormat.name());
            }

            /**
             * Sets the max buffer size for a BYTE transformer.
             *
             * @param maxBufferSize
             *            the maximum buffer size.
             * @return this
             */
            public Setter maxBufferSize(final int maxBufferSize) {
                return setValue(TRANSFORMER_MAX_BUFFER_SIZE, maxBufferSize);
            }
        }
    }
}
