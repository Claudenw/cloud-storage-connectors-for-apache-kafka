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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.common.OffsetManager;

import com.google.common.base.Objects;

public final class S3OffsetManagerEntry implements OffsetManager.OffsetManagerEntry<S3OffsetManagerEntry> {

    // package private statics for testing.
    // TODO make this package private after values in S3SourceTask are no longer needed
    public static final String BUCKET = "bucket";
    public static final String OBJECT_KEY = "objectKey";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    static final String RECORD_COUNT = "recordCount";

    /**
     * THe list of Keys that may not be set via {@link #setProperty(String, Object)}.
     */
    static final List<String> RESTRICTED_KEYS = List.of(BUCKET, OBJECT_KEY, TOPIC, PARTITION, RECORD_COUNT);
    /** The data map that stores all the values */
    private final Map<String, Object> data;
    /** THe record count for the data map. Extracted here because it is used/updated frequently duirng processing */
    private long recordCount;

    /**
     * Construct the S3OffsetManagerEntry.
     *
     * @param bucket
     *            the bucket we are using.
     * @param s3ObjectKey
     *            the S3Object key.
     * @param topic
     *            The topic we are using.
     * @param partition
     *            the partition we are using.
     */
    public S3OffsetManagerEntry(final String bucket, final String s3ObjectKey, final String topic,
            final Integer partition) {
        data = new HashMap<>();
        data.put(BUCKET, bucket);
        data.put(OBJECT_KEY, s3ObjectKey);
        data.put(TOPIC, topic);
        data.put(PARTITION, partition);
    }

    /**
     * Constructs an OffsetManagerEntry from an existing map. used by {@link #fromProperties(Map)}. Package private for
     * testing
     *
     * @param properties
     *            the property map.
     */
    private S3OffsetManagerEntry(final Map<String, Object> properties) {
        data = new HashMap<>(properties);
        for (final String field : RESTRICTED_KEYS) {
            if (data.get(field) == null) {
                throw new IllegalArgumentException("Missing '" + field + "' property");
            }
        }
    }

    /**
     * Creates an S3OffsetManagerEntry. Will return {@code null} if properties is {@code null}.
     *
     * @param properties
     *            the properties to wrap. May be {@code null}.
     * @return an S3OffsetManagerEntry.
     * @throws IllegalArgumentException
     *             if one of the {@link #RESTRICTED_KEYS} is missing.
     */
    @Override
    public S3OffsetManagerEntry fromProperties(final Map<String, Object> properties) {
        if (properties == null) {
            return null;
        }
        final Map<String, Object> ourProperties = new HashMap<>(properties);
        final Long recordCount = (Long) ourProperties.computeIfAbsent(RECORD_COUNT, s -> 0L);

        final S3OffsetManagerEntry result = new S3OffsetManagerEntry(ourProperties);
        result.recordCount = recordCount;
        return result;
    }

    @Override
    public Object getProperty(final String key) {
        if (RECORD_COUNT.equals(key)) {
            return recordCount;
        }
        return data.get(key);
    }

    @Override
    public void setProperty(final String property, final Object value) {
        if (RESTRICTED_KEYS.contains(property)) {
            throw new IllegalArgumentException(
                    String.format("'%s' is a restricted key and may not be set using setProperty()", property));
        }
        data.put(property, value);
    }

    @Override
    public void incrementRecordCount() {
        recordCount++;
    }

    /**
     * Gets the umber of records extracted from data returned from S3.
     *
     * @return the umber of records extracted from data returned from S3.
     */
    public long getRecordCount() {
        return recordCount;
    }

    /**
     * Gets the S3Object key for the current object.
     *
     * @return the S3ObjectKey.
     */
    public String getKey() {
        return (String) data.get(OBJECT_KEY);
    }

    @Override
    public Integer getPartition() {
        return (Integer) data.get(PARTITION);
    }

    @Override
    public String getTopic() {
        return (String) data.get(TOPIC);
    }

    /**
     * Gets the S3 bucket for the current object.
     *
     * @return the S3 Bucket for the current object.
     */
    public String getBucket() {
        return (String) data.get(BUCKET);
    }
    /**
     * Creates a new offset map. No defensive copy is necessary.
     *
     * @return a new map of properties and values.
     */
    @Override
    public Map<String, Object> getProperties() {
        final Map<String, Object> result = new HashMap<>(data);
        result.put(RECORD_COUNT, recordCount);
        return result;
    }

    /**
     * Returns the OffsetManagerKey for this Entry.
     *
     * @return the OffsetManagerKey for this Entry.
     */
    @Override
    public OffsetManager.OffsetManagerKey getManagerKey() {
        return () -> Map.of(BUCKET, data.get(BUCKET), TOPIC, data.get(TOPIC), PARTITION, data.get(PARTITION),
                OBJECT_KEY, data.get(OBJECT_KEY));
    }

    @Override
    public boolean equals(final Object other) {
        if (other instanceof S3OffsetManagerEntry) {
            return compareTo((S3OffsetManagerEntry) other) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getBucket(), getTopic(), getPartition());
    }

    @Override
    public int compareTo(final S3OffsetManagerEntry other) {
        if (this == other) { // NOPMD comparing instance
            return 0;
        }
        int result = ((String) getProperty(BUCKET)).compareTo((String) other.getProperty(BUCKET));
        if (result == 0) {
            result = getTopic().compareTo(other.getTopic());
            if (result == 0) {
                result = getPartition().compareTo(other.getPartition());
                if (result == 0) { // NOPMD deeply nested if.
                    result = getKey().compareTo(other.getKey());
                    if (result == 0) {
                        result = Long.compare(getRecordCount(), other.getRecordCount());
                    }
                }
            }
        }
        return result;
    }
}