/*
 * Copyright 2025 Aiven Oy
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

/**
 * Information about the Native object.
 *
 * @param <N> The native object type.
 * @param <K> the native key type
 */
public interface NativeInfo<N, K> {
    /**
     * Gets the native item.
     *
     * @return The native item.
     */
    N getNativeItem();

    /**
     * Gets the native key
     *
     * @return The Native key.
     */
    K getNativeKey();

    /**
     * Gets the number of bytes in the input stream extracted from the native object.
     *
     * @return The number of bytes in the input stream extracted from the native object.
     */
    long getNativeItemSize();
}
