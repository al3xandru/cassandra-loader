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


import java.lang.String;
import java.lang.Long;
import java.lang.Number;
import java.util.Locale;
import java.text.ParseException;
import java.lang.IndexOutOfBoundsException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

// Long parser - use the Number parser
public class LongParser extends NumberParser {
    public LongParser() {
        super();
    }

    public LongParser(Locale inLocale) {
        super(inLocale);
    }

    public LongParser(Locale inLocale, Boolean grouping) {
        super(inLocale, grouping);
    }

    public Long parse(String toparse) throws ParseException {
        Number val = super.parse(toparse);
        return (null == val) ? null : val.longValue();
    }
}
