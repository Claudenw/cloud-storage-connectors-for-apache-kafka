package io.aiven.kafka.connect.s3.source.utils;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3ObjectToSourceRecordMapperTest {


    private Transformer transformer;

    private S3SourceConfig s3SourceConfig;

    private TopicPartitionExtractingPredicate topicPartitionExtractingPredicate;

    private OffsetManager offsetManager;

    private S3ObjectToSourceRecordMapper underTest;


    private S3OffsetManagerEntry offsetManagerEntry;


    private S3Object s3Object = mock(S3Object.class);

    @BeforeEach
    public void setUp() {
        transformer = mock(Transformer.class);
        topicPartitionExtractingPredicate = mock(TopicPartitionExtractingPredicate.class);
        offsetManagerEntry = new S3OffsetManagerEntry("bucket", "s3ObjectKey", "topic", 1);
        when(topicPartitionExtractingPredicate.getOffsetMapEntry()).thenReturn(offsetManagerEntry);
        s3SourceConfig = mock(S3SourceConfig.class);
        s3Object = mock(S3Object.class);
        when(s3Object.getKey()).thenReturn("s3ObjectKey");
        offsetManager = mock(OffsetManager.class);
    }

    @Test
    void noS3ObjectContentContentTest() {
        underTest = new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig, offsetManager);
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result.hasNext()).isFalse();
    }

    void assertS3RecordMatches(S3SourceRecord s3SourceRecord, String key, String value) {
        assertThat(s3SourceRecord.getRecordKey()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));
        assertThat(s3SourceRecord.getRecordValue()).isEqualTo(value.getBytes(StandardCharsets.UTF_8));
    }

    private S3ObjectToSourceRecordMapper singleRecordMapper(final Transformer transformer) {
        S3ObjectInputStream inputStream = new S3ObjectInputStream(new ByteArrayInputStream("Original value".getBytes(StandardCharsets.UTF_8)),
                mock(HttpRequestBase.class));
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        return new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig, offsetManager);
    }

    private S3ObjectToSourceRecordMapper doubleRecordMapper() {
        S3ObjectInputStream inputStream = new S3ObjectInputStream(new ByteArrayInputStream("Original value".getBytes(StandardCharsets.UTF_8)),
                mock(HttpRequestBase.class));
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        return new S3ObjectToSourceRecordMapper(new TestingTransformer() {
            @Override
            public List<Object> getRecords(InputStream inputStream, String topic, int topicPartition, S3SourceConfig s3SourceConfig) {
                List<Object> result = super.getRecords(inputStream, topic, topicPartition, s3SourceConfig);
                result.add("Transformed2");
                return result;
            }
        }, topicPartitionExtractingPredicate, s3SourceConfig, offsetManager);
    }

    @Test
    void singleRecordTest() {
        underTest = singleRecordMapper(new TestingTransformer());
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result).hasNext();
        S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertThat(offsetManagerEntry.getRecordCount()).isEqualTo(1L);
        assertThat(result.hasNext()).isFalse();
    }


    @Test
    void multipleRecordTest() {
        underTest = doubleRecordMapper();
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result).hasNext();
        S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertThat(offsetManagerEntry.getRecordCount()).isEqualTo(1L);
        assertThat(result).hasNext();
        s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed2");
        assertThat(offsetManagerEntry.getRecordCount()).isEqualTo(2L);
    }

    @Test
    void transformerRuntimeExceptionTest() {
        underTest = singleRecordMapper(new TestingTransformer() {
            @Override
            public List<Object> getRecords(InputStream inputStream, String topic, int topicPartition, S3SourceConfig s3SourceConfig) {
                throw new RuntimeException("BOOM!");
            }
        });
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result.hasNext()).isFalse();
    }

    @Test
    void transformerIOExceptionTest() {
        InputStream baseInputStream = new ByteArrayInputStream("Original value".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public void close() throws IOException {
                throw new IOException("BOOM!");
            }
        };
        S3ObjectInputStream inputStream = new S3ObjectInputStream(baseInputStream, mock(HttpRequestBase.class));
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        underTest = new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig, offsetManager);
        underTest = singleRecordMapper(new TestingTransformer());
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result).hasNext();
        S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertThat(offsetManagerEntry.getRecordCount()).isEqualTo(1L);
        assertThat(result.hasNext()).isFalse();
    }
}
