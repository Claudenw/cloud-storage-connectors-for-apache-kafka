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

package io.aiven.kafka.connect.s3.source.utils;

import static io.aiven.kafka.connect.s3.source.S3SourceTask.BUCKET;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.PARTITION;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.TOPIC;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.FETCH_PAGE_SIZE;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.LOGGER;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.aiven.kafka.connect.s3.source.output.ByteMapper;
import io.aiven.kafka.connect.s3.source.utils.iterators.ExtendedIterator;
import io.aiven.kafka.connect.s3.source.utils.iterators.NullIterator;
import io.aiven.kafka.connect.s3.source.utils.iterators.WrappedIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.OutputWriter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator  {
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";
    public static final String OFFSET_KEY = "offset";

    private static final  long DEFAULT_START_OFFSET_ID = 0L;

    private static final int DEFAULT_PARTITION = 0;

    public static final int PAGE_SIZE_FACTOR = 2;

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern
            .compile("(?<topicName>[^/]+?)-" + "(?<partitionId>\\d{5})" + "\\.(?<fileExtension>[^.]+)$"); // ex :
    // topic-00001.txt
    private String currentObjectKey;

    private Iterator<S3ObjectSummary> nextFileIterator;
    private Iterator<List<ConsumerRecord<byte[], byte[]>>> recordIterator = Collections.emptyIterator();

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final AmazonS3 s3Client;

    private final OutputWriter outputWriter;

    private ExtendedIterator<Iterator<ConsumerRecord<byte[], byte[]>>> consumerRecordIterIter;

    private final ExtendedIterator<S3ObjectSummary> s3ObjectSummaryIterator;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
                                final OffsetManager offsetManager, final OutputWriter outputWriter) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.outputWriter = outputWriter;

        s3ObjectSummaryIterator = WrappedIterator.create(new Iterator<S3ObjectSummary>() {
            Iterator<S3ObjectSummary> innerIterator = new S3ObjectSummaryIterator(s3Client, bucketName, s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR);

            @Override
            public boolean hasNext() {
                if (!innerIterator.hasNext()) {
                    innerIterator = new S3ObjectSummaryIterator(s3Client, bucketName, s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR);
                }
                return innerIterator.hasNext();
            }

            @Override
            public S3ObjectSummary next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return innerIterator.next();
            }
        });
    }





    private S3Object objectMap(S3ObjectSummary s3ObjectSummary) {
        return s3Client.getObject(bucketName, s3ObjectSummary.getKey());
    }


    private Iterator<ConsumerRecord<byte[], byte[]>> ConsumerRecordMap(final S3Object s3Object) {
        final String topic;
        final int topicPartition;
        final byte[] key = s3Object.getKey() == null ? null : s3Object.getKey().getBytes(StandardCharsets.UTF_8);

        try {
            if (key == null) {
                topic = offsetManager.getFirstConfiguredTopic(s3SourceConfig);
                topicPartition = DEFAULT_PARTITION;
            } else {
                final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3Object.getKey());
                if (fileMatcher.find()) {
                    topic = fileMatcher.group(PATTERN_TOPIC_KEY);
                    topicPartition = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));
                } else {
                    topic = offsetManager.getFirstConfiguredTopic(s3SourceConfig);
                    topicPartition = DEFAULT_PARTITION;
                }
            }


            final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(topic, topicPartition,
                    bucketName);

            Map<Map<String, Object>, Long> currentOffsets = new HashMap<>();

            try (InputStream inputStream = s3Object.getObjectContent()) {
                ExtendedIterator<byte[]> valueBytesIterator = WrappedIterator.create(outputWriter.toByteArray(inputStream, topic));
                return valueBytesIterator.filter(bytes -> bytes.length > 0)
                        .map(value -> {
                            long currentOffset;
                            if (offsetManager.getOffsets().containsKey(partitionMap)) {
                                currentOffset = offsetManager.incrementAndUpdateOffsetMap(partitionMap);
                            } else {
                                currentOffset = currentOffsets.getOrDefault(partitionMap, DEFAULT_START_OFFSET_ID);
                                currentOffsets.put(partitionMap, currentOffset + 1);
                            }
                            return new ConsumerRecord<>(topic, topicPartition, currentOffset, key, value);
                        });
            }
        } catch (IOException e) {
            LOGGER.error("can not create consumer record for S3Object", e);
            return Collections.emptyIterator();
        }
    }


    public AivenS3SourceRecord aivenS3SourceRecordMap(ConsumerRecord<byte[], byte[]> currentRecord) {

        Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(BUCKET, bucketName);
        partitionMap.put(TOPIC, currentRecord.topic());
        partitionMap.put(PARTITION, currentRecord.partition());

        // Create the offset map
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(OFFSET_KEY, currentRecord.offset());

        return new AivenS3SourceRecord(partitionMap, offsetMap, currentRecord.topic(),
                currentRecord.partition(), currentRecord.key(), currentRecord.value());
    }

    public boolean hasData() {
        return s3ObjectSummaryIterator.hasNext();
    }

    public ExtendedIterator<AivenS3SourceRecord> forMaxPollRecords(int maxPollRecords) {
        ExtendedIterator<AivenS3SourceRecord> result = new NullIterator<>();
        if (hasData()) {
        int i=0;
            ExtendedIterator<Iterator<ConsumerRecord<byte[], byte[]>>> innerIterator = WrappedIterator.create(s3ObjectSummaryIterator).map(this::objectMap).map(this::ConsumerRecordMap);
        while (i<maxPollRecords && innerIterator.hasNext()) {
            i++;
            result.andThen(WrappedIterator.create(innerIterator.next()).map(this::aivenS3SourceRecordMap));
        }
        return result;
    }

    public ExtendedIterator<AivenS3SourceRecord> allRecords() {
        if (hasData()) {
            ExtendedIterator<Iterator<ConsumerRecord<byte[], byte[]>>> innerIterator = WrappedIterator.create(s3ObjectSummaryIterator).map(this::objectMap).map(this::ConsumerRecordMap);
            return WrappedIterator.create(new Iterator<AivenS3SourceRecord>() {
                Iterator<AivenS3SourceRecord> outerIterator = WrappedIterator.create(innerIterator.next()).map(SourceRecordIterator.this::aivenS3SourceRecordMap);


                @Override
                public boolean hasNext () {
                    if (!outerIterator.hasNext()) {
                        if (!innerIterator.hasNext()) {
                            return false;
                        }
                        outerIterator = WrappedIterator.create(innerIterator.next()).map(SourceRecordIterator.this::aivenS3SourceRecordMap);
                    }
                    return outerIterator.hasNext();
                }

                @Override
                public AivenS3SourceRecord next () {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return outerIterator.next();
                }
            });
        }
        return new NullIterator<>();
    }

}
