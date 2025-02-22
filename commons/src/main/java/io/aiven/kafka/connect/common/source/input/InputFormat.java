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

package io.aiven.kafka.connect.common.source.input;

import java.util.Locale;

public enum InputFormat {
    AVRO("avro"), PARQUET("parquet"), JSONL("jsonl"), BYTES("bytes");

    private final String format;

    InputFormat(final String format) {
        this.format = format;
    }

    public String getValue() {
        return format.toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return format;
    }
}
