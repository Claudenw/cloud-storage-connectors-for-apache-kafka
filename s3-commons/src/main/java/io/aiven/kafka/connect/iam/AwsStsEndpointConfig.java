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

package io.aiven.kafka.connect.iam;

import java.util.Objects;

public final class AwsStsEndpointConfig {
    public static final String AWS_STS_GLOBAL_ENDPOINT = "https://sts.amazonaws.com";

    private final String serviceEndpoint;
    private final String signingRegion;

    public AwsStsEndpointConfig(final String serviceEndpoint, final String signingRegion) {
        this.serviceEndpoint = serviceEndpoint;
        this.signingRegion = signingRegion;
    }

    public String getServiceEndpoint() {
        return serviceEndpoint;
    }

    public String getSigningRegion() {
        return signingRegion;
    }

    public Boolean isValid() {
        return Objects.nonNull(signingRegion);
    }
}
