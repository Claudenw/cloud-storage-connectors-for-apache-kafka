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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.OffsetManager;
import io.aiven.kafka.connect.common.source.input.parquet.LocalInputFile;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transformer to read parquet files. The input file is read one Avro record at a time. This implementation creates a
 * temporary file to read the data from, and removes it at the end of processing.
 */
public class ParquetTransformer extends Transformer {
    /** The AvroData to read with */
    private final AvroData avroData;
    /** The logger for this transform */
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetTransformer.class);

    /**
     * Constructs a ParquetTransformer that reads with the AvroData.
     *
     * @param avroData
     *            The AvroData to read with.
     */
    ParquetTransformer(final AvroData avroData) {
        super();
        this.avroData = avroData;
    }

    @Override
    public Schema getKeySchema() {
        return null;
    }

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final OffsetManager.OffsetManagerEntry<?> offsetManagerEntry, final AbstractConfig sourceConfig) {

        return new StreamSpliterator(LOGGER, inputStreamIOSupplier, offsetManagerEntry) {

            private ParquetReader<GenericRecord> reader;
            private File parquetFile;

            @Override
            protected InputStream inputOpened(final InputStream input) throws IOException {
                try {
                    // Create a temporary file for the Parquet data
                    parquetFile = File.createTempFile(
                            String.format("%s_%s", offsetManagerEntry.getTopic(), offsetManagerEntry.getPartition()),
                            ".parquet");
                } catch (IOException e) {
                    LOGGER.error("Error creating temp file for Parquet data: {}", e.getMessage(), e);
                    throw e;
                }

                try (OutputStream outputStream = Files.newOutputStream(parquetFile.toPath())) {
                    IOUtils.copy(input, outputStream); // Copy input stream to temporary file
                }
                reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(parquetFile.toPath())).build();
                return input;
            }

            @Override
            protected void doClose() {
                if (reader != null) {
                    try {
                        reader.close(); // Close reader at end of file
                    } catch (IOException e) {
                        logger.error("Error closing reader: {}", e.getMessage(), e);
                    }
                }
                if (parquetFile != null) {
                    deleteTmpFile(parquetFile.toPath());
                }
            }

            @Override
            protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                try {
                    final GenericRecord record = reader.read();
                    if (record != null) {
                        action.accept(avroData.toConnectData(record.getSchema(), record)); // Pass record to the stream
                        return true;
                    }
                } catch (IOException e) {
                    logger.error("Error reading record: {}", e.getMessage(), e);
                }
                return false;
            }
        };
    }

    /**
     * Deletes the temporary file.
     *
     * @param parquetFile
     *            the temporary file to delete.
     */
    static void deleteTmpFile(final Path parquetFile) {
        if (Files.exists(parquetFile)) {
            try {
                Files.delete(parquetFile);
            } catch (IOException e) {
                LOGGER.error("Error in deleting tmp file {}", e.getMessage(), e);
            }
        }
    }
}