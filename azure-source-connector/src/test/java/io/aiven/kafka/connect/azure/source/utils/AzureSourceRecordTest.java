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

package io.aiven.kafka.connect.azure.source.utils;

import com.azure.storage.blob.models.BlobItemProperties;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordTest;

import com.azure.storage.blob.models.BlobItem;

class AzureSourceRecordTest extends AbstractSourceRecordTest<BlobItem, String, AzureOffsetManagerEntry, AzureBlobSourceRecord> {
    public static final String TEST_BLOB_NAME_TXT = "topic-00001-1741965423180.txt";
    public static final String CONTAINER = "container1";

    @Override
    protected String createKFrom(String key) {
        return key;
    }

    @Override
    protected AzureOffsetManagerEntry createOffsetManagerEntry(String key) {
        return new AzureOffsetManagerEntry(CONTAINER, key);
    }

    @Override
    protected AzureBlobSourceRecord createSourceRecord() {
        BlobItem blobItem = new BlobItem();
        blobItem.setName(TEST_BLOB_NAME_TXT);
        BlobItemProperties blobItemProperties = new BlobItemProperties();
        blobItemProperties.setContentLength(5L);
        blobItem.setProperties(blobItemProperties);
        return new AzureBlobSourceRecord(blobItem);
    }
}
