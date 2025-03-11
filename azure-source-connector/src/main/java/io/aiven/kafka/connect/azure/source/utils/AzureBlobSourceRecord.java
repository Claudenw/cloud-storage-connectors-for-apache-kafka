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

package io.aiven.kafka.connect.azure.source.utils;

import io.aiven.kafka.connect.common.source.AbstractSourceRecord;

import com.azure.storage.blob.models.BlobItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobSourceRecord extends AbstractSourceRecord<BlobItem, String, AzureOffsetManagerEntry> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSourceRecord.class);


    public AzureBlobSourceRecord(final BlobItem blobItem) {
        super(blobItem);
    }

    public AzureBlobSourceRecord(final AzureBlobSourceRecord azureBlobSourceRecord) {
        super(azureBlobSourceRecord);
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @Override
    public String getNativeKey() {
        return getNativeItem().getName();
    }

    @Override
    public long getNativeItemSize() {
        return getNativeItem().getProperties().getContentLength();
    }

    @Override
    public AzureBlobSourceRecord  duplicate() {
        return new AzureBlobSourceRecord(this);
    }

}
