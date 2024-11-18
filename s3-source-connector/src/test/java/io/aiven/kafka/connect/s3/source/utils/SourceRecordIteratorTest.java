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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class SourceRecordIteratorTest {

    private AmazonS3 mockS3Client;
    private S3SourceConfig mockConfig;
    private OffsetManager mockOffsetManager;
    private Transformer mockTransformer;

    private FileReader mockFileReader;

    private List<S3ObjectSummary> s3ObjectSummaries;

    private Map<String, S3Object> s3ObjectMap;

    private String getObjectContents(String bucket, String key) {
        return String.format("Hello from %s on %s", key, bucket);
    }

    private void createS3Object(S3ObjectSummary summary) {
        S3Object result = new S3Object();
        result.setKey(summary.getKey());
        result.setBucketName(summary.getBucketName());
        result.setObjectContent(new ByteArrayInputStream(getObjectContents(summary.getBucketName(), summary.getKey()).getBytes(StandardCharsets.UTF_8)));
        s3ObjectMap.put(summary.getKey(), result);
    }
    private S3ObjectSummary createS3ObjectSummary(String key) {
        S3ObjectSummary result = new S3ObjectSummary();
        result.setKey(key);
        result.setBucketName("bucket");
        createS3Object(result);
        return result;
    }

    @BeforeEach
    public void setUp() {
        s3ObjectSummaries = new ArrayList<>();
        s3ObjectMap = new HashMap<>();
        mockS3Client = mock(AmazonS3.class);
        when(mockS3Client.getObject(anyString(), anyString())).thenAnswer(invocation -> s3ObjectMap.get(invocation.getArguments()[1]));
        mockConfig = mock(S3SourceConfig.class);
        mockOffsetManager = mock(OffsetManager.class);
        mockTransformer = mock(Transformer.class);
    }

    void assertS3RecordMatches(S3SourceRecord s3SourceRecord, String key, String value) {
        assertArrayEquals(key.getBytes(StandardCharsets.UTF_8), s3SourceRecord.getRecordKey(), key);
        assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), s3SourceRecord.getRecordValue(), value);
    }

    @Test
    void allObjectsTest() throws Exception {
        String[] keys = {"topic-00001-key1.txt", "topic-00001-key2.txt", "topic-00001-key3.txt"};

        s3ObjectSummaries.add(createS3ObjectSummary(keys[0]));
        s3ObjectSummaries.add(createS3ObjectSummary(keys[1]));
        s3ObjectSummaries.add(createS3ObjectSummary(keys[2]));

        Iterator<S3ObjectSummary> s3ObjectSummaryIterator = s3ObjectSummaries.iterator();

        SourceRecordIterator sourceRecordIterator = new SourceRecordIterator(mockConfig, mockS3Client, "bucket", mockOffsetManager, new TestingTransformer(), s3ObjectSummaryIterator);
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];

            assertTrue(sourceRecordIterator.hasNext());
            S3SourceRecord sourceRecord = sourceRecordIterator.next();
            assertS3RecordMatches(sourceRecord, key, TestingTransformer.transform(getObjectContents("bucket", key)));
        }

        assertFalse(sourceRecordIterator.hasNext());
    }

    @Test
    void skipObjectsTest() throws Exception {
        String[] keys = {"topic-00001-key1.txt", "topic-00001-key2.txt", "topic-00001-key3.txt"};

        s3ObjectSummaries.add(createS3ObjectSummary(keys[0]));
        s3ObjectSummaries.add(createS3ObjectSummary(keys[1]));
        s3ObjectSummaries.add(createS3ObjectSummary(keys[2]));

        Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();

        S3OffsetManagerEntry offsetManagerEntry = new S3OffsetManagerEntry("bucket", keys[1], "topic", 1);
        offsetManagerEntry.incrementRecordCount();
        offsets.put(offsetManagerEntry.getManagerKey().getPartitionMap(), offsetManagerEntry.getProperties());

        mockOffsetManager = new OffsetManager(offsets);


        Iterator<S3ObjectSummary> s3ObjectSummaryIterator = s3ObjectSummaries.iterator();

        SourceRecordIterator sourceRecordIterator = new SourceRecordIterator(mockConfig, mockS3Client, "bucket", mockOffsetManager, new TestingTransformer(), s3ObjectSummaryIterator);
        for (int i = 1; i < keys.length; i++) {
            String key = keys[i];

            assertTrue(sourceRecordIterator.hasNext(), "at position "+i);
            S3SourceRecord sourceRecord = sourceRecordIterator.next();
            assertS3RecordMatches(sourceRecord, key, TestingTransformer.transform(getObjectContents("bucket", key)));
        }

        assertFalse(sourceRecordIterator.hasNext());
    }


//
//        final ListObjectsV2Result result = mockListObjectsResult(mockObjectSummaries);
//        when(mockS3Client.listObjectsV2(anyString())).thenReturn(result);
//
//        // Mock S3Object and InputStream
//        try (S3Object mockS3Object = mock(S3Object.class);
//                S3ObjectInputStream mockInputStream = new S3ObjectInputStream(new ByteArrayInputStream(new byte[] {}),
//                        null);) {
//            when(mockS3Client.getObject(anyString(), anyString())).thenReturn(mockS3Object);
//            when(mockS3Object.getObjectContent()).thenReturn(mockInputStream);
//
//            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any()))
//                    .thenReturn(Collections.singletonList(new Object()));
//
//            final String outStr = "this is a test";
//            when(mockTransformer.getValueBytes(any(), anyString(), any()))
//                    .thenReturn(outStr.getBytes(StandardCharsets.UTF_8));
//
//            when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());
//
//            when(mockFileReader.fetchObjectSummaries(any())).thenReturn(Collections.emptyIterator());
//            SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockS3Client, "test-bucket",
//                    mockOffsetManager, mockTransformer, mockFileReader);
//
//            assertFalse(iterator.hasNext());
//
//            when(mockFileReader.fetchObjectSummaries(any())).thenReturn(mockObjectSummaries.listIterator());
//
//            iterator = new SourceRecordIterator(mockConfig, mockS3Client, "test-bucket", mockOffsetManager,
//                    mockTransformer, mockFileReader);
//
//            assertTrue(iterator.hasNext());
//            assertNotNull(iterator.next());
//        }
//
//    private ListObjectsV2Result mockListObjectsResult(final List<S3ObjectSummary> summaries) {
//        final ListObjectsV2Result result = mock(ListObjectsV2Result.class);
//        when(result.getObjectSummaries()).thenReturn(summaries);
//        return result;
//    }
}
