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

import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Consumer;

import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * An implementation of the SourceRecordIteratorTest.
 */
final class SourceRecordIteratorTest extends AbstractSourceRecordIteratorTest<S3Object, String, S3OffsetManagerEntry, S3SourceRecord> {
    /** the client */
    private AWSV2SourceClient sourceApiClient;
    /** tA client builder */
    private S3ClientBuilder s3ClientBuilder;
    /** The client that we build the iterator from */
    private S3Client s3Client;

    @Override
    protected String createKFrom(final String key) {
        // native Ky is a string so just rturn the arg.
        return key;
    }

    @Override
    protected AbstractSourceRecordIterator<S3Object, String, S3OffsetManagerEntry, S3SourceRecord> createSourceRecordIterator(final SourceCommonConfig mockConfig, final OffsetManager<S3OffsetManagerEntry> mockOffsetManager, final Transformer mockTransformer) {
        // create an instance of our concrete iterator.
        return new SourceRecordIterator((S3SourceConfig) mockConfig, mockOffsetManager,  mockTransformer, new AWSV2SourceClient(s3Client, (S3SourceConfig) mockConfig));
    }

    @Override
    protected ClientMutator<S3Object, String, S3ClientBuilder> createClientMutator() {
        // create our ClientMutator instance.
        return  new S3ClientBuilder();
    }

    @Override
    protected SourceCommonConfig createMockedConfig(final String filePattern) {
        // create a mocked config with the values required by the S3CSourceConfig
        FileNameFragment mockFileNameFrag = mock(FileNameFragment.class);
        S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        when(mockFileNameFrag.getFilenameTemplate()).thenReturn(Template.of(filePattern));
        when(s3SourceConfig.getFilenameTemplate()).thenReturn(Template.of(filePattern));
        when(s3SourceConfig.getS3FetchBufferSize()).thenReturn(1);
        when(s3SourceConfig.getAwsS3BucketName()).thenReturn("testBucket");
        when(s3SourceConfig.getFetchPageSize()).thenReturn(10);
        return s3SourceConfig;
    }

    @Override
    protected SourceCommonConfig createConfig(final Map<String, String> data) {
        // create an instance of the S3SourceCOnfig.
        data.put(AWS_S3_BUCKET_NAME_CONFIG, "bucket-name");
        return new S3SourceConfig(data);
    }

////    @ParameterizedTest
////    @CsvSource({ "4, 2, key1", "4, 3, key2", "4, 0, key3", "4, 1, key4" })
////    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdAssigned(final int maxTasks, final int taskId,
////            final String objectKey) {
////
////        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
////        final String filePattern = "{{partition}}";
////        final String topic = "topic";
////        mockSourceConfig(mockConfig, filePattern, taskId, maxTasks, topic);
////        final S3Object obj = S3Object.builder().key(objectKey).build();
////        sourceApiClient = mock(AWSV2SourceClient.class);
////
////        final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
////                sourceApiClient);
////
////        final Predicate<S3Object> s3ObjectPredicate = s3Object -> iterator.taskAssignment
////                .test(iterator.fileMatching.apply(s3Object));
////        assertThat(s3ObjectPredicate).accepts(obj);
////
////    }
//
////    @ParameterizedTest
////    @CsvSource({ "4, 1, topic1-2-0", "4, 3,key1", "4, 0, key1", "4, 1, key2", "4, 2, key2", "4, 0, key2", "4, 1,key3",
////            "4, 2, key3", "4, 3, key3", "4, 0, key4", "4, 2, key4", "4, 3, key4" })
////    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdUnassigned(final int maxTasks, final int taskId,
////            final String objectKey) {
////        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
////        final String filePattern = "{{partition}}";
////        final String topic = "topic";
////        mockSourceConfig(mockConfig, filePattern, taskId, maxTasks, topic);
////        final S3Object obj = S3Object.builder().key(objectKey).build();
////        sourceApiClient = mock(AWSV2SourceClient.class);
////
////        final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
////                sourceApiClient);
////        final Predicate<S3Object> s3ObjectPredicate = s3Object -> iterator.taskAssignment
////                .test(iterator.fileMatching.apply(s3Object));
////        // Assert
////        assertThat(s3ObjectPredicate.test(obj)).as("Predicate should accept the objectKey: " + objectKey).isFalse();
////    }


    /**
     * The mutator implementation.
     */
    class S3ClientBuilder extends ClientMutator<S3Object, String, S3ClientBuilder> {

        @Override
        protected S3Object createObject(final String key, final ByteBuffer data) {
            return S3Object.builder().key(key).size((long) data.capacity()).build();
        }

        /**
         * Creates an  S3 ResponseBytes object from the key and the data for that key.
         * In this implementation the native key is a string so we just use String here.
         * @param key the key to build the response for.
         * @return the ResponseBytes object for the key.
         */
        private ResponseBytes getResponse(final String key) {
            return ResponseBytes.fromByteArray(new byte[0], getData(key).array());
        }

        /**
         * Create a S3 ListObjectV2Respone object from a single block.
         * @return the new ListObjectV2Response
         */
        private ListObjectsV2Response dequeueData() {
            // Dequeue a block.  Sets the objects.
            dequeueBlock();
            return ListObjectsV2Response.builder().contents(objects).isTruncated(false).build();
        }

        @Override
        public void build() {
            // if there are objects create the last block from them.
            if (!objects.isEmpty()) {
                endOfBlock();
            }
            // create an S3Client.
            s3Client = mock(S3Client.class);
            // when a listObjectV2 is requests deququ the answer from the blocks.
            when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(env -> dequeueData());
            when(s3Client.listObjectsV2(any(Consumer.class))).thenAnswer(env -> dequeueData());
            // when an objectRequest is sent retrieve the response data.
            when(s3Client.getObjectAsBytes(any(GetObjectRequest.class)))
                    .thenAnswer(env -> getResponse(env.getArgument(0, GetObjectRequest.class).key()));
        }
    }

}

