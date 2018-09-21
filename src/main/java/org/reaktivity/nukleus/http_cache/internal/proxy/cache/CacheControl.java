/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Apache Version 2.0 (July 25, 2017)
// https://svn.apache.org/repos/asf/abdera/java/trunk/
// core/src/main/java/org/apache/abdera/protocol/util/CacheControlUtil.java

public class CacheControl
{
    private static final String REGEX =
            "\\s*([\\w\\-]+)\\s*(=)?\\s*(\\d+|\\\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)+\\\")?\\s*";

    private static final Pattern CACHE_DIRECTIVES = Pattern.compile(REGEX);

    private final HashMap<String, String> values = new LinkedHashMap<>();

    public CacheControl()
    {

    }

    public CacheControl parse(String value)
    {
        values.clear();
        if (value != null)
        {
            Matcher matcher = CACHE_DIRECTIVES.matcher(value);
            while (matcher.find())
            {
                String directive = matcher.group(1);
                values.put(directive, matcher.group(3));
            }
        }
        return this;
    }

    public Iterator<String> iterator()
    {
        return values.keySet().iterator();
    }

    public Map<String, String> getValues()
    {
        return values;
    }

    public boolean contains(String directive)
    {
        return values.containsKey(directive);
    }

    public String getValue(String directive)
    {
        return values.get(directive);
    }
}
