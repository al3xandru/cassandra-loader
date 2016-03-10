/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.loader.parser;


import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class ListParser extends AbstractParser {
    private Parser parser;
    private char collectionDelim;
    private char collectionBegin;
    private char collectionEnd;
    private char collectionQuote = '\"';
    private char collectionEscape = '\\';
    private String collectionNullString = "null";
    private List<Object> elements;

    public ListParser(Parser inParser, char inCollectionDelim,
                      char inCollectionBegin, char inCollectionEnd) {
        parser = inParser;
        collectionDelim = inCollectionDelim;
        collectionBegin = inCollectionBegin;
        collectionEnd = inCollectionEnd;
        elements = new ArrayList<Object>();
    }


    @Override
    public Object parse(String toparse) throws ParseException {
        if (null == toparse) {
            return null;
        }
        if (!toparse.startsWith(Character.toString(collectionBegin))) {
            throw new ParseException("Must begin with " + collectionBegin
                    + "\n", 0);
        }
        if (!toparse.endsWith(Character.toString(collectionEnd))) {
            throw new ParseException("Must end with " + collectionEnd
                    + "\n", 0);
        }
        toparse = toparse.substring(1, toparse.length() - 1);
        IndexedLine sr = new IndexedLine(toparse);
        String parseit;
        elements.clear();
        try {
            while (null != (parseit = super.getQuotedOrUnquoted(sr,
                    collectionNullString,
                    collectionDelim,
                    collectionEscape,
                    collectionQuote))) {
                elements.add(parser.parse(parseit));
            }
        }
        catch(IOException ioe) {
            System.err.println("Trouble parsing : " + ioe.getMessage());
            return null;
        }
        return elements;
    }

    @Override
    public String getQuotedOrUnquoted(IndexedLine il, String nullString, Character delim, Character escape, Character quote)
    throws IOException, ParseException {
        if (null == delim) {
            return null;
        }
        if (!il.hasNext()) {
            return null;
        }
        char c = il.getNext();
        if (c == delim) {
            return "";
        }
        if ((null != quote && c != quote) && (c != collectionBegin)) {
            throw new ParseException("List must begin with '\"' or '[' (found '" + c + "')", 0);
        }
        boolean isQuoted = false;
        StringBuilder sb = new StringBuilder(10240);
        if (null != quote && c == quote) {
            isQuoted = true;
            sb.append(c);
            if (!il.hasNext() || (collectionBegin != (c = il.getNext()))) {
                throw new ParseException("List starts with '['", 1);
            }
        }
        else if (collectionBegin != c) {
            throw new ParseException("A list must begin with '['", 0);
        }
        boolean insideCol = true;
        boolean insideQuote = false;
        sb.append(c);
        while (il.hasNext()) {
            c = il.getNext();
            if ((c == delim) && !insideQuote && !insideCol && !isQuoted) {
                break;
            }
            sb.append(c);
            if (c == collectionEnd && !insideQuote) {
                insideCol = false;
            }
            if (null != quote && c == quote) {
                if (!insideCol) {
                    isQuoted = false;
                }
                else {
                    insideQuote = !insideQuote;
                }
            }
        }
        return prepareToParse(sb.toString(), nullString, quote);
    }

    @SuppressWarnings("unchecked")
    public String format(Object o) {
        List<Object> list = (List<Object>) o;
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        sb.append(collectionBegin);
        if (list.size() > 0) {
            for(int i = 0; i < list.size() - 1; i++) {
                sb.append(parser.format(list.get(i)));
                sb.append(collectionDelim);
            }
            sb.append(parser.format(list.get(list.size() - 1)));
        }
        sb.append(collectionEnd);
        sb.append("\"");
        return sb.toString();
    }
}
