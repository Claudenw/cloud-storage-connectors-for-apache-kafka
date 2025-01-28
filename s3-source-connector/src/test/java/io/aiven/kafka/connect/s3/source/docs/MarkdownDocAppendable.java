package io.aiven.kafka.connect.s3.source.docs;

import org.apache.commons.lang3.StringUtils;
import scala.Char;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MarkdownDocAppendable extends BaseDocAppendable {

    private static final String ESCAPED_CHARS = "\\`*_{}[]<>()#+-.!|";

    /**
     * Constructs an appendable filter built on top of the specified underlying appendable.
     *
     * @param output the underlying appendable to be assigned to the field {@code this.output} for later use, or {@code null} if this instance is to be created
     *               without an underlying stream.
     */
    protected MarkdownDocAppendable(Appendable output) {
        super(output);
    }

    @Override
    public void appendHeader(int level, CharSequence text) throws IOException {
        append(String.format("%s %s%n", Util.repeat(level, '#'), escape(text)));
    }

    @Override
    public void appendList(boolean ordered, Collection<CharSequence> list) throws IOException {
        String prefix = ordered ? "1." : "-";
        list.stream().forEach(s -> {
            try {
                append(String.format("%s %s%n", prefix, escape(s)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public String escape(CharSequence charSequence) {
        String result = charSequence.toString();
        for (char c : ESCAPED_CHARS.toCharArray()) {
            result = result.replace(String.valueOf(c), "\\"+c);
        }
        return result;
    }

    @Override
    public void appendParagraph(CharSequence paragraph) throws IOException {
        append(String.format("%S%n%n", paragraph));
    }

    private String escapeTableEntry(String text) {
        return escape(text).replace("\n", "<br/>");
    }

    @Override
    public void appendTable(TableDefinition table) throws IOException {
        final char eol = '\n';
        append("| ");
        for (String header : table.headers()) {
            append(escapeTableEntry(header)).append(" | ");
        }
        append(eol).append("| ");
        for (TextStyle style : table.columnTextStyles()) {
            switch (style.getAlignment()) {

                case LEFT:
                    append(":--- ");
                    break;
                case CENTER:
                    append(":---: ");
                    break;
                case RIGHT:
                    append("---: ");
                    break;
            }
            append("| ");
        }
        append(eol);
        for (List<String> rows : table.rows()) {
            append("| ");
            for (String column : rows) {
                append(escapeTableEntry(column)).append(" | ");
            }
            append(eol);
        }

    }

    @Override
    public void appendTitle(CharSequence title) throws IOException {
        this.appendTitleAndSidebar(title, null);
    }

    public void appendTitleAndSidebar(CharSequence title, CharSequence sidebar) throws IOException {
        append(String.format("--- %ntitle: %s %nsidebar: %s %n---%n", title, StringUtils.defaultIfEmpty(sidebar, title)));
    }
}
