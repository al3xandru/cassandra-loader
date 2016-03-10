package com.datastax.loader.parser;


import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author alex <alex@mypopescu.com>
 * @since 3/6/16
 */
public class CollectionParserTest {
    private static final IgnoreParser ignore = new IgnoreParser();

    @Test
    @SuppressWarnings("unchecked")
    public void parseSimpleList() throws IOException, ParseException {
        IndexedLine line = new IndexedLine("ignore,\"[1,2,3]\",\"[\"a\",\"b\",\"c\"]\",ignore");
        ignore.parse(line, "", ',', '\\', '"', false);
        ListParser listParser = new ListParser(new StringParser(), ',', '[', ']');
        List<Object> val = (List<Object>) listParser.parse(line, "", ',', '\\', '"', false);
        assertEquals("[1, 2, 3]", String.valueOf(val));
        val = (List<Object>) listParser.parse(line, "", ',', '\\', '"', false);
        assertEquals("[a, b, c]", String.valueOf(val));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void parseListWithCommas() throws IOException, ParseException {
        IndexedLine line = new IndexedLine("ignore,\"[\"a,b\",\"cd\"]\",ignore");
        ignore.parse(line, "", ',', '\\', '"', false);
        ListParser listParser = new ListParser(new StringParser(), ',', '[', ']');
        List<Object> val = (List<Object>) listParser.parse(line, "", ',', '\\', '"', false);
        assertEquals("[a,b, cd]", String.valueOf(val));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void parseUnquotedList() throws IOException, ParseException {
        IndexedLine line = new IndexedLine("ignore,[1, 2, 3],[\"a\",\"b\",\"c\"],[\"a,b\",\"c]d\",ef],ignored");
        ignore.parse(line, "", ',', '\\', '"', false);
        ListParser intListParser = new ListParser(new IntegerParser(Locale.ENGLISH), ',', '[', ']');
        ListParser strListParser = new ListParser(new StringParser(), ',', '[', ']');
        List<Object> actualVal = (List<Object>) intListParser.parse(line, "", ',', '\\', '"', false);
        assertEquals("[1, 2, 3]", String.valueOf(actualVal));
        actualVal = (List<Object>) strListParser.parse(line, "", ',', '\\', '"', false);
        assertEquals("[a, b, c]", String.valueOf(actualVal));
        actualVal = (List<Object>) strListParser.parse(line, "", ',', '\\', '"', false);
        assertEquals("[a,b, c]d, ef]", String.valueOf(actualVal));
    }

    @Test
    @Ignore
    public void parseSetWithCommas() throws IOException, ParseException {
        IndexedLine line = new IndexedLine("ignore,{\"a,b\", \"c,d\"]}ignore");
        ignore.parse(line, "", ',', '\\', '"', false);
        SetParser setParser = new SetParser(new StringParser(), ',', '{', '}');
        Set<Object> val = (Set<Object>) setParser.parse(line, "", ',', '\\', '"', false);
        assertEquals("the string value", val);
    }

    @Test
    @Ignore
    public void parseMapWithCommas() throws IOException, ParseException {
        IndexedLine line = new IndexedLine("ignore,{\"a,b\": 1, \"c,d\": 2},ignore");
        ignore.parse(line, "", ',', '\\', '"', false);
        MapParser mapParser = new MapParser(new StringParser(), new IntegerParser(Locale.ENGLISH), ',', '{', '}', ':');
        Map<Object, Object> val = (Map<Object, Object>) mapParser.parse(line, "", ',', '\\', '"', false);
        assertEquals("the string value", val);
    }
}
