package io.aiven.kafka.connect.s3.source.docs;

import io.aiven.kafka.connect.docs.ConfigDocumentation;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class S3SourceConfigDoc {
    @Test
    public void generate() throws IOException {
        ConfigDocumentation.main(new String[]{"-c", S3SourceConfig.class.getName(), "-f", "TEXT",
                "-o", "docs/configs/S3SourceConfig.md" } );

        ConfigDocumentation.main(new String[]{"-c", S3SourceConfig.class.getName(), "-f", "YAML",
                "-o", "docs/configs/S3SourceConfig.yml" } );
    }
}
