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

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.common.templating.Template;

public class SourceCommonConfig extends CommonConfig {

    private final TransformerFragment transformerFragment;
    private final SourceConfigFragment sourceConfigFragment;
    private final FileNameFragment fileNameFragment;
    private final OutputFormatFragment outputFormatFragment;

    public SourceCommonConfig(ConfigDef definition, Map<?, ?> originals) {// NOPMD
        super(definition, originals);
        // Construct Fragments
        transformerFragment = new TransformerFragment(this);
        sourceConfigFragment = new SourceConfigFragment(this);
        fileNameFragment = new FileNameFragment(this);
        outputFormatFragment = new OutputFormatFragment(this);

        validate(); // NOPMD ConstructorCallsOverridableMethod
    }

    private void validate() {
        transformerFragment.validate();
        sourceConfigFragment.validate();
        fileNameFragment.validate();
        outputFormatFragment.validate();
    }

    public InputFormat getInputFormat() {
        return transformerFragment.getInputFormat();
    }

    public String getSchemaRegistryUrl() {
        return transformerFragment.getSchemaRegistryUrl();
    }

    public String getTargetTopic() {
        return sourceConfigFragment.getTargetTopic();
    }

    public ErrorsTolerance getErrorsTolerance() {
        return sourceConfigFragment.getErrorsTolerance();
    }

    public DistributionType getDistributionType() {
        return sourceConfigFragment.getDistributionType();
    }

    public int getMaxPollRecords() {
        return sourceConfigFragment.getMaxPollRecords();
    }

    public Transformer getTransformer() {
        return TransformerFactory.getTransformer(transformerFragment.getInputFormat());
    }

    public int getTransformerMaxBufferSize() {
        return transformerFragment.getTransformerMaxBufferSize();
    }

    public Template getFilenameTemplate() {
        return fileNameFragment.getFilenameTemplate();
    }

}
