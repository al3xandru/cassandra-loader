package com.datastax.loader.parser;


import java.io.IOException;
import java.text.ParseException;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Alex Popescu
 */
public class StringParserTest {
    @Test
    public void parseString() throws ParseException, IOException {
        IndexedLine line = new IndexedLine("ignore,the string value,ignore");
        IgnoreParser ignore = new IgnoreParser();
        ignore.parse(line, "", ',', '\"', '\\', false);
        StringParser parser = new StringParser();
        String val = (String) parser.parse(line, "", ',', '\\', '\"', false);
        Assert.assertEquals("the string value", val);
    }

    @Test
    public void parseQuotedString() throws ParseException, IOException {
        IndexedLine line = new IndexedLine("ignore,\"the string value\",\"a string with , comma\",\" quoted string with space \"");
        IgnoreParser ignore = new IgnoreParser();
        ignore.parse(line, "", ',', '\"', '\\', false);
        StringParser parser = new StringParser();
        String val = (String) parser.parse(line, "", ',', '\\', '\"', false);
        Assert.assertEquals("the string value", val);
        val = (String) parser.parse(line, "", ',', '\\', '\"', false);
        Assert.assertEquals("a string with , comma", val);
        val = (String) parser.parse(line, "", ',', '\\', '\"', true);
        Assert.assertEquals(" quoted string with space ", val);
    }

    @Test
    public void parseStringWithQuotes() throws ParseException, IOException {
        IndexedLine line = new IndexedLine("ignore,\"the \"\"string\"\" value\",ignore");
        IgnoreParser ignore = new IgnoreParser();
        ignore.parse(line, "", ',', '\"', '\\', false);
        StringParser parser = new StringParser();
        String val = (String) parser.parse(line, "", ',', '\\', '\"', false);
        Assert.assertEquals("the \"string\" value", val);
    }

    @Test
    public void writeStringWithQuotes() {
        StringParser parser = new StringParser();
        Assert.assertEquals("the \"\"quoted\"\" string", parser.format("the \"quoted\" string"));
    }

    @Test
    public void writeStringWithSpace() {
        StringParser parser = new StringParser();
        Assert.assertEquals("\" starts and ends with space  \"",
                parser.format(" starts and ends with space  "));
    }

    @Test
    public void writeStringWithComma() {
        StringParser parser = new StringParser();
        Assert.assertEquals("\"one, two,three\"", parser.format("one, two,three"));

    }

    @Test
    public void writeStringWithNewline() {
        StringParser parser = new StringParser();
        Assert.assertEquals("\"a newline\ncharacter\"", parser.format("a newline\ncharacter"));
    }


    private static class IgnoreParser extends AbstractParser {
        @Override
        public Object parse(String toparse) throws ParseException {
            return null;
        }

        @Override
        public String format(Object o) {
            return "";
        }
    }
}
