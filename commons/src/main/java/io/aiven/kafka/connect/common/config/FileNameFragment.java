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

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.FilenameTemplateValidator;
import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.config.validators.TimestampSourceValidator;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.templating.Template;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Fragment to handle all file name extraction operations. Requires {@link OutputFormatFragment} and (@link
 * CompressionFragment}
 */
public final class FileNameFragment extends ConfigFragment {

    // package private so that testing can access.
    static final String GROUP_FILE = "File";
    static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    static final String FILE_MAX_RECORDS = "file.max.records";
    static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    static final String DEFAULT_FILENAME_TEMPLATE = "{{topic}}-{{partition}}-{{start_offset}}";

    public static final String FILE_PATH_PREFIX_TEMPLATE_CONFIG = "file.prefix.template";

    /**
     * Gets a setter for this properties in this fragment.
     * @param data the data to update.
     * @return the Setter.
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Create an instance of this fragment wrapping the specified config.
     * @param cfg the configuration to read from.
     */
    public FileNameFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Adds the FileName properties to the configuration definition.
     *
     * @param configDef
     *            the configuration definition to update.
     * @return the updated configuration definition.
     */
    public static ConfigDef update(final ConfigDef configDef) {
        int fileGroupCounter = 0;

        configDef.define(FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null,
                new FilenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG), ConfigDef.Importance.MEDIUM,
                "The template for file names on S3. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                        + "(the offset of the first record in the file). "
                        + "Only some combinations of variables are valid, which currently are:\n"
                        + "- `topic`, `partition`, `start_offset`."
                        + "There is also `key` only variable {{key}} for grouping by keys",
                GROUP_FILE, ++fileGroupCounter, ConfigDef.Width.LONG, FILE_NAME_TEMPLATE_CONFIG);

        final String supportedCompressionTypes = CompressionType.names()
                .stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));

        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, null, new FileCompressionTypeValidator(),
                ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on S3. " + "The supported values are: "
                        + supportedCompressionTypes + ".",
                GROUP_FILE, ++fileGroupCounter, ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));

        configDef.define(FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, new ConfigDef.Validator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                assert value instanceof Integer;
                if ((Integer) value < 0) {
                    throw new ConfigException(FILE_MAX_RECORDS, value, "must be a non-negative integer number");
                }
            }
        }, ConfigDef.Importance.MEDIUM,
                "The maximum number of records to put in a single file. " + "Must be a non-negative integer number. "
                        + "0 is interpreted as \"unlimited\", which is the default.",
                GROUP_FILE, ++fileGroupCounter, ConfigDef.Width.SHORT, FILE_MAX_RECORDS);

        configDef.define(FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING, ZoneOffset.UTC.toString(),
                new TimeZoneValidator(), ConfigDef.Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                GROUP_FILE, ++fileGroupCounter, 
                ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_TIMEZONE);

        configDef.define(FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING, TimestampSource.Type.WALLCLOCK.name(),
                new TimestampSourceValidator(), ConfigDef.Importance.LOW,
                "Specifies the the timestamp variable source. Default is wall-clock.", GROUP_FILE, ++fileGroupCounter,
                ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_SOURCE);

        return configDef;
    }

    @Override
    public void validate() {
        // Special checks for {{key}} filename template.
        final Template filenameTemplate = getFilenameTemplate();
        final String groupType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        if (isKeyBased(groupType) && getMaxRecordsPerFile() > 1) {
            final String msg = String.format("When %s is %s, %s must be either 1 or not set", FILE_NAME_TEMPLATE_CONFIG,
                    filenameTemplate, FILE_MAX_RECORDS);
            throw new ConfigException(msg);
        }
    }

    private Boolean isKeyBased(final String groupType) {
        return RecordGrouperFactory.KEY_RECORD.equals(groupType)
                || RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD.equals(groupType);
    }

    /**
     * Returns the text of the filename template. May throw {@link ConfigException} if the property
     * {@link #FILE_NAME_TEMPLATE_CONFIG} is not set.
     *
     * @return the text of the filename template.
     */
    public String getFilename() {
        if (has(FILE_NAME_TEMPLATE_CONFIG)) {
            return cfg.getString(FILE_NAME_TEMPLATE_CONFIG);
        }
        final CompressionType compressionType = new CompressionFragment(cfg).getCompressionType();
        return FormatType.AVRO.equals(new OutputFormatFragment(cfg).getFormatType())
                ? DEFAULT_FILENAME_TEMPLATE + ".avro" + compressionType.extension()
                : DEFAULT_FILENAME_TEMPLATE + compressionType.extension();
    }

    /**
     * Gets the filename template.
     *
     * @return the Filename template.
     */
    public Template getFilenameTemplate() {
        return Template.of(getFilename());
    }

    /**
     * Gets the filename timezone
     *
     * @return The timezone specified for filenames.
     */
    public ZoneId getFilenameTimezone() {
        return ZoneId.of(cfg.getString(FILE_NAME_TIMESTAMP_TIMEZONE));
    }

    /**
     * Gets the timestamp source for the file name.
     *
     * @return the timestamp source for the file name.
     */
    public TimestampSource getFilenameTimestampSource() {
        return TimestampSource.of(getFilenameTimezone(),
                TimestampSource.Type.of(cfg.getString(FILE_NAME_TIMESTAMP_SOURCE)));
    }

    /**
     * Gets the maximum number of records allowed in a file.
     *
     * @return the maximum number of records allowed in a file.
     */
    public int getMaxRecordsPerFile() {
        return cfg.getInt(FILE_MAX_RECORDS);
    }

    /**
     * Setter for the FileNameFragment.
     */
    public static final class Setter extends AbstractFragmentSetter<Setter> {
        /**
         * Constructs the Setter.
         * @param data the data to update.
         */
        private Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Sets the file compression type.
         * @param compressionType the compression type.
         * @return this
         */
        public Setter fileCompression(final CompressionType compressionType) {
            return setValue(FILE_COMPRESSION_TYPE_CONFIG, compressionType.name());
        }

        /**
         * Sets the maximum records per file.
         * @param maxRecordsPerFile the maximum records per file.
         * @return this.
         */
        public Setter maxRecordsPerFile(final int maxRecordsPerFile) {
            return setValue(FILE_MAX_RECORDS, maxRecordsPerFile);
        }

        /**
         * Sets the time stamp source.
         * @param timestampSource the time stamp source.
         * @return this.
         */
        public Setter timestampSource(final TimestampSource timestampSource) {
            return setValue(FILE_NAME_TIMESTAMP_SOURCE, timestampSource.type().name());
        }

        /**
         * Sets the timestamp source from a type.
         * @param type the type to set the timestamp source to.
         * @return this.
         */
        public Setter timestampSource(final TimestampSource.Type type) {
            return setValue(FILE_NAME_TIMESTAMP_SOURCE, type.name());
        }

        /**
         * Sets the timestamp timezone.
         * @param timeZone the timezone to se.t
         * @return this
         */
        public Setter timestampTimeZone(final ZoneId timeZone) {
            return setValue(FILE_NAME_TIMESTAMP_TIMEZONE, timeZone.toString());
        }

        /**
         * Sets the file name template.
         * @param template the prefix template to use.
         * @return this.
         */
        public Setter template(final String template) {
            return setValue(FILE_NAME_TEMPLATE_CONFIG, template);
        }

        /**
         * Sets the file name prefix template.
         * @param prefixTemplate the prefix template to use.
         * @return this
         */
        public Setter prefixTemplate(final String prefixTemplate) {
            return setValue(FILE_PATH_PREFIX_TEMPLATE_CONFIG, prefixTemplate);
        }
    }
}
