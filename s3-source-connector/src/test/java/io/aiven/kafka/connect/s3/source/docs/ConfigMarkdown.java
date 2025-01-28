package io.aiven.kafka.connect.s3.source.docs;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ConfigMarkdown {
    ConfigDef config = S3SourceConfig.configDef();

    @Test
    public void x() throws IOException {
        Collection< ConfigDef.ConfigKey> keys = config.configKeys().values();


        List<String> headers = Arrays.asList("Name", "Display Name", "Default", "Description");
        List<TextStyle> styles = Arrays.asList(TextStyle.DEFAULT, TextStyle.DEFAULT, TextStyle.DEFAULT, TextStyle.DEFAULT);
        List<List<String>> rows = new ArrayList<>();
        for (ConfigDef.ConfigKey key : keys) {
            rows.add(Arrays.asList(key.name, key.displayName, key.defaultValue == null ? "" : key.defaultValue.toString(), key.documentation));
        }

        StringBuilder sb = new StringBuilder();
        TableDefinition tableDefinition = TableDefinition.from("", styles, headers, rows);

        MarkdownDocAppendable output = new MarkdownDocAppendable(sb);
        output.appendTable(tableDefinition);
        System.out.println(sb);
    }

}
