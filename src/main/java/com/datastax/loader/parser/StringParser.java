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


// String parser - simple
public class StringParser extends AbstractParser {
    public String parse(String toparse) {
        return toparse;
    }

    public String format(Object o) {
        if (null == o) {
            return "";
        }
        String v = String.valueOf(o).replace("\"", "\"\"");
        if (Character.isSpaceChar(v.charAt(0)) || Character.isSpaceChar(v.charAt(v.length() - 1))) {
            return '"' + v + '"';
        }
        if (v.indexOf(",") > -1 || v.indexOf("\n") > -1 || v.indexOf("\r") > -1) {
            return '"' + v + '"';
        }
        return v;
    }
}
