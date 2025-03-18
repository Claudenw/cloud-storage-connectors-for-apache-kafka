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

package io.aiven.kafka.connect.common.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.task.Context;

import org.junit.jupiter.api.Test;

/**
 * Tests an AbstractSourceRecord implementation.
 *
 * @param <N>
 *            The Native object type.
 * @param <K>
 *            The native key type.
 * @param <O>
 *            The OffsetManagerEntry type.
 * @param <T>
 *            The concrete implementation of the {@link AbstractSourceRecord} .
 */
public abstract class AbstractSourceRecordTest<N, K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<N, K, O, T>> {

    public static final String TEST_OBJECT_KEY_TXT = "test-object-key.txt";

    // abstract methods

    /**
     * Convert a string into the key value for the native object. In most cases the underlying system uses a string so
     * returning the {@code key} argument is appropriate. However, this method provides an opportunity to convert the
     * key into something that the native system would produce.
     *
     * @param key
     *            the key value as a string.
     * @return the native key equivalent of the {@code key} parameter.
     */
    abstract protected K createKFrom(final String key);

    /**
     * Create an offset manager entry from the string key value,
     *
     * @param key
     *            the key value as a string.
     * @return an OffsetManager entry.
     */
    abstract protected O createOffsetManagerEntry(final String key);

    /**
     * Creates the source record under test.
     *
     * @return the source record under test.
     */
    abstract protected T createSourceRecord();

    @Test
    void testCreateSourceRecord() {
        final String topic = "test-topic";
        final O offsetManagerEntry = createOffsetManagerEntry(TEST_OBJECT_KEY_TXT);
        final Context<K> context = new Context<K>(createKFrom(TEST_OBJECT_KEY_TXT));
        context.setPartition(null);
        context.setTopic(topic);

        final T sourceRecord = createSourceRecord();
        sourceRecord.setOffsetManagerEntry(offsetManagerEntry);
        sourceRecord.setContext(context);
        sourceRecord.setValueData(new SchemaAndValue(null, ""));
        sourceRecord.setKeyData(new SchemaAndValue(null, ""));

        final OffsetManager<O> offsetManager = (OffsetManager<O>) mock(OffsetManager.class);

        final SourceRecord result = sourceRecord.getSourceRecord(ErrorsTolerance.NONE, offsetManager);
        assertThat(result).isNotNull();
        assertThat(result.topic()).isNotNull();
        assertThat(result.topic()).isEqualTo(topic);
        assertThat(result.kafkaPartition()).isEqualTo(null);
    }

    @Test
    void testCreateSourceRecordWithDataError() {
        final Context<K> context = new Context<K>(createKFrom(TEST_OBJECT_KEY_TXT));
        final O mockOffsetManagerEntry = (O) mock(OffsetManager.OffsetManagerEntry.class);
        when(mockOffsetManagerEntry.getManagerKey()).thenThrow(new DataException("Test Exception"));
        when(mockOffsetManagerEntry.fromProperties(any())).thenReturn(mockOffsetManagerEntry);

        final OffsetManager<O> offsetManager = (OffsetManager<O>) mock(OffsetManager.class);

        final T sourceRecord = createSourceRecord();
        sourceRecord.setOffsetManagerEntry(mockOffsetManagerEntry);
        sourceRecord.setContext(context);

        assertThatExceptionOfType(ConnectException.class).as("Errors tolerance: NONE")
                .isThrownBy(() -> sourceRecord.getSourceRecord(ErrorsTolerance.NONE, offsetManager));
        final SourceRecord result = sourceRecord.getSourceRecord(ErrorsTolerance.ALL, offsetManager);
        assertThat(result).isNull();
    }

    @Test
    void testModifyingInitialContextDoesNotAlterTheSourceRecordsContext() {
        final String topic = "test-topic";
        final O offsetManagerEntry = createOffsetManagerEntry(TEST_OBJECT_KEY_TXT);
        Context<K> context = new Context<K>(createKFrom(TEST_OBJECT_KEY_TXT));
        context.setPartition(null);
        context.setTopic(topic);

        final T sourceRecord = createSourceRecord();
        sourceRecord.setOffsetManagerEntry(offsetManagerEntry);
        sourceRecord.setContext(context);
        sourceRecord.setValueData(new SchemaAndValue(null, ""));
        sourceRecord.setKeyData(new SchemaAndValue(null, ""));

        final OffsetManager<O> offsetManager = (OffsetManager<O>) mock(OffsetManager.class);

        // alter context, it should have no impact on the source record.
        context.setPartition(14);
        context.setTopic("a-diff-topic");
        SourceRecord result = sourceRecord.getSourceRecord(ErrorsTolerance.NONE, offsetManager);
        assertThat(result).isNotNull();
        assertThat(result.topic()).isEqualTo(topic);
        assertThat(result.kafkaPartition()).isEqualTo(null);

        // We should return a defensive copy so altering here should not affect the ssSourceRecord
        context = sourceRecord.getContext();
        context.setPartition(99);
        context.setTopic("another-diff-topic");
        result = sourceRecord.getSourceRecord(ErrorsTolerance.NONE, offsetManager);
        assertThat(result).isNotNull();
        assertThat(result.topic()).isEqualTo(topic);
        assertThat(result.kafkaPartition()).isEqualTo(null);

    }

    @Test
    void testModifyingInitialOffsetManagerEntryDoesNotAlterTheSourceRecordsOffsetManagerEntry() {
        final String topic = "test-topic";
        O offsetManagerEntry = createOffsetManagerEntry(TEST_OBJECT_KEY_TXT);
        Context<K> context = new Context<K>(createKFrom(TEST_OBJECT_KEY_TXT));
        context.setPartition(null);
        context.setTopic(topic);

        final T sourceRecord = createSourceRecord();
        sourceRecord.setOffsetManagerEntry(offsetManagerEntry);
        sourceRecord.setContext(context);
        sourceRecord.setValueData(new SchemaAndValue(null, ""));
        sourceRecord.setKeyData(new SchemaAndValue(null, ""));
        final long currentRecordCount = offsetManagerEntry.getRecordCount();
        // alter entry record count and it should have no impact on the source record.
        offsetManagerEntry.incrementRecordCount();
        assertThat(sourceRecord.getRecordCount()).isEqualTo(currentRecordCount);

        // We should return a defensive copy so altering here should not affect the ssSourceRecord
        offsetManagerEntry = sourceRecord.getOffsetManagerEntry();
        offsetManagerEntry.incrementRecordCount();
        offsetManagerEntry.incrementRecordCount();
        assertThat(sourceRecord.getRecordCount()).isEqualTo(currentRecordCount);
    }

}
