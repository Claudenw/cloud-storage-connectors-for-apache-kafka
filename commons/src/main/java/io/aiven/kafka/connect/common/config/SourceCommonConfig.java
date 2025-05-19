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

package io.aiven.kafka.connect.common.config;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.task.DistributionType;

public class SourceCommonConfig extends CommonConfig implements SourceConfigAccess, TransformerAccess {

    private final OutputFormatFragment outputFormatFragment;

    public SourceCommonConfig(ConfigDef definition, Map<?, ?> originals) {// NOPMD
        super(definition, originals);
        // Construct Fragments
        outputFormatFragment = new OutputFormatFragment(this);

        validate(); // NOPMD ConstructorCallsOverridableMethod
    }

    private void validate() {
        SourceConfigAccess.validate(this);
        TransformerAccess.validate(this);
        outputFormatFragment.validate();
    }

    @Override
    public AbstractConfig getAbstractConfig() {
        return this;
    }
}
