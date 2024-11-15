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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.MAX_POLL_RECORDS;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {

    private final Iterator<S3ObjectSummary> s3ObjectSummaryIterator;

    private final TopicPartitionExtractingPredicate topicPartitionExtractingPredicate;
    private final S3ObjectToSourceRecordMapper sourceToRecordMapper;

    private Iterator<S3SourceRecord> outer;
    private final Iterator<Iterator<S3SourceRecord>> inner;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final OffsetManager offsetManager, final Transformer transformer, final FileReader fileReader) {
        s3ObjectSummaryIterator = fileReader.fetchObjectSummaries(s3Client);
        topicPartitionExtractingPredicate = new TopicPartitionExtractingPredicate();
        sourceToRecordMapper = new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig, offsetManager);
        Iterator<S3ObjectSummary> objectSummaryIterator = IteratorUtils.filteredIterator(s3ObjectSummaryIterator, topicPartitionExtractingPredicate::test);
        inner = IteratorUtils.transformedIterator(objectSummaryIterator, s3ObjectSummary -> sourceToRecordMapper.apply(s3Client.getObject(bucketName, s3ObjectSummary.getKey())));
        outer = null;
    }

    @Override
    public boolean hasNext() {
        if (outer == null) {
            if (inner.hasNext()) {
                outer = inner.next();
            } else {
                return false;
            }
        }
        while (!outer.hasNext()) {
            if (inner.hasNext()) {
                outer = inner.next();
            } else {
                outer = null;
                return false;
            }
        }
        return true;
    }

    @Override
    public S3SourceRecord next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return outer.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

}
