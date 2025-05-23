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

package io.aiven.kafka.connect.common.source.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.task.Context;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonTransformer extends Transformer {

    private final JsonConverter jsonConverter;

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTransformer.class);

    final ObjectMapper objectMapper = new ObjectMapper();

    JsonTransformer(final JsonConverter jsonConverter) {
        super();
        this.jsonConverter = jsonConverter;
    }

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final long streamLength, final Context<?> context, final SourceCommonConfig sourceConfig) {
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
            BufferedReader reader;

            @Override
            protected void inputOpened(final InputStream input) {
                reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));

            }

            @Override
            public void doClose() {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        LOGGER.error("Error closing reader: {}", e.getMessage(), e);
                    }
                }
            }

            @Override
            public boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                String line = null;
                try {
                    // remove blank and empty lines.
                    while (StringUtils.isBlank(line)) {
                        line = reader.readLine();
                        if (line == null) {
                            // end of file
                            return false;
                        }
                    }
                    line = line.trim();
                    // toConnectData does not actually use topic in the conversion so its fine if it is null.
                    action.accept(jsonConverter.toConnectData(context.getTopic().orElse(null),
                            line.getBytes(StandardCharsets.UTF_8)));
                    return true;
                } catch (IOException e) {
                    LOGGER.error("Error reading input stream: {}", e.getMessage(), e);
                    return false;
                }
            }
        };
    }

    @Override
    public SchemaAndValue getKeyData(final Object cloudStorageKey, final String topic,
            final SourceCommonConfig sourceConfig) {
        return new SchemaAndValue(null, ((String) cloudStorageKey).getBytes(StandardCharsets.UTF_8));
    }
}
