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

package io.aiven.kafka.connect.s3.source.output;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OutputWriter {
    /**
     * Configure the OutputWriter
     * @param config  the configuration
     * @param s3SourceConfig the S3Source configuration.
     */
    void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig);

    /**
     * Converts an input stream into an {@code Iterator<byte[]>} where each {@code byte[]} is the data to be sent to kafka.
     * @param inputStream the input stream to read bytes from.
     * @param topic the Topic the input stream is to be associated with.
     * @return an Iterator of buffers extracted from the input stream.
     */
    Iterator<byte[]> toByteArray(InputStream inputStream, String topic);

}
