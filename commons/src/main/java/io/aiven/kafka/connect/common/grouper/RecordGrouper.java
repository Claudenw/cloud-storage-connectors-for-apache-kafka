/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.grouper;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * The interface for classes that associates {@link SinkRecord}s with files by some criteria.
 */
public interface RecordGrouper {
    /**
     * Associate the record with the appropriate file.
     *
     * @param record
     *            - record to group
     */
    void put(SinkRecord record);

    /**
     * Clear all records.
     */
    void clear();

    /**
     * Get all records associated with files, grouped by the file name.
     *
     * @return map of records associated with files
     */
    Map<String, List<SinkRecord>> records();

    interface Rotator<T> {

        boolean rotate(T target);

    }

}
