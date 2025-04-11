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

package io.aiven.kafka.connect.azure.source.config;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;

public class AzureBlobSourceConfigDef extends ConfigDef {

    public AzureBlobSourceConfigDef() {
        super();
        AzureBlobConfigFragment.update(this);
        SourceConfigFragment.update(this);
        TransformerFragment.update(this);
        OutputFormatFragment.update(this, OutputFieldType.VALUE);
    }

    @Override
    public List<ConfigValue> validate(final Map<String, String> props) { // NOPMD overriding method just calls super.
                                                                         // (so far)
        // TODO review if this required later before release
        return super.validate(props);
    }
}
