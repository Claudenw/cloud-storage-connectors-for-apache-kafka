package io.aiven.kafka.connect.s3.source.docs;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConfigMarkdown {
    ConfigDef config = S3SourceConfig.configDef();

    @Test
    public void x() throws IOException {
        Collection< ConfigDef.ConfigKey> keys = config.configKeys().values();
        int maxColumnWidth = 35;
        int tableWidth = 180;

        List<String> headers = Arrays.asList("Name", "Default", "Description");
        int maxName = 0;
        int maxValue = 0;
        Map<String, List<String>> rows = new TreeMap<>();
        for (ConfigDef.ConfigKey key : keys) {
            maxName = maxName > key.name.length() ? maxName : key.name.length();
            String value = key.defaultValue == null ? "" : key.defaultValue.toString();
            maxValue = maxValue > value.length() ? maxValue : value.length();
            rows.put(key.name, Arrays.asList(key.name, value, key.documentation));
        }

        maxName = maxName > maxColumnWidth ? maxColumnWidth : maxName;
        maxValue = maxValue > maxColumnWidth ? maxColumnWidth : maxValue;
        int maxDesc = tableWidth - maxName - maxValue;
        List<TextStyle> styles = Arrays.asList(
                TextStyle.builder().setMaxWidth(maxName).get(),
                TextStyle.builder().setMaxWidth(maxValue).get(),
                TextStyle.builder().setMaxWidth(maxDesc).get());

        StringBuilder sb = new StringBuilder();
        TableDefinition tableDefinition = TableDefinition.from("", styles, headers, rows.values());

        MarkdownDocAppendable output = new MarkdownDocAppendable(sb);
        output.appendTable(tableDefinition);
        System.out.println(sb);
    }

}
