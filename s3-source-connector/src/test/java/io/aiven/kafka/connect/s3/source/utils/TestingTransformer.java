package io.aiven.kafka.connect.s3.source.utils;

import com.nimbusds.jose.util.IOUtils;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestingTransformer implements Transformer {

    public static String transform(String original) {
        return String.format("Transformed(%s)", original);
    }
    @Override
    public void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig) {
        config.put("TestingTransformer", "True");
    }

    @Override
    public List<Object> getRecords(InputStream inputStream, String topic, int topicPartition, S3SourceConfig s3SourceConfig) {
        List<Object> result = new ArrayList<>();
        try {
            result.add(transform(IOUtils.readInputStreamToString(inputStream)));
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Error reading input stream", e);
        }
    }

    @Override
    public byte[] getValueBytes(Object record, String topic, S3SourceConfig s3SourceConfig) {
        return ((String) record).getBytes(StandardCharsets.UTF_8);
    }
}
