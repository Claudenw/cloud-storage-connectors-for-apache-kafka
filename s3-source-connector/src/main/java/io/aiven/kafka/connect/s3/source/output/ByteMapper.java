package io.aiven.kafka.connect.s3.source.output;

import io.aiven.kafka.connect.s3.source.utils.iterators.ExtendedIterator;

import java.io.InputStream;
import java.util.Iterator;

@FunctionalInterface
public interface ByteMapper {
    /**
     * Converts an input stream into an {@code Iterator<byte[]>} where each {@code byte[]} is the data to be sent to kafka.
     * @param inputStream the input stream to read bytes from.
     * @param topic the Topic the input stream is to be associated with.
     * @return an Iterator of buffers extracted from the input stream.
     */
    Iterator<byte[]> toByteArray(InputStream inputStream, String topic);
}
