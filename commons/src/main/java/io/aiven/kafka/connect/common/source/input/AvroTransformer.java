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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.task.Context;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroTransformer extends Transformer {

    private final AvroData avroData;

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroTransformer.class);

    AvroTransformer(final AvroData avroData) {
        super();
        this.avroData = avroData;
    }

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final long streamLength, final Context<?> context, final SourceCommonConfig sourceConfig) {
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
            private DataFileStream<GenericRecord> dataFileStream;
            private final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

            @Override
            protected void inputOpened(final InputStream input) throws IOException {
                dataFileStream = new DataFileStream<>(input, datumReader);
            }

            @Override
            public void doClose() {
                if (dataFileStream != null) {
                    try {
                        dataFileStream.close();
                    } catch (IOException e) {
                        LOGGER.error("Error closing reader: {}", e.getMessage(), e);
                    }
                }
            }

            @Override
            protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                if (dataFileStream.hasNext()) {
                    final GenericRecord record = dataFileStream.next();
                    action.accept(avroData.toConnectData(record.getSchema(), record));
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    public SchemaAndValue getKeyData(final Object cloudStorageKey, final String topic,
            final SourceCommonConfig sourceConfig) {
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA,
                ((String) cloudStorageKey).getBytes(StandardCharsets.UTF_8));
    }
}
