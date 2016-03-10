package com.datastax.loader.parser;


import java.text.ParseException;

/**
 * @author alex <alex@mypopescu.com>
 * @since 3/6/16
 */
class IgnoreParser extends AbstractParser {
    @Override
    public Object parse(String toparse) throws ParseException {
        return null;
    }

    @Override
    public String format(Object o) {
        return "";
    }
}
