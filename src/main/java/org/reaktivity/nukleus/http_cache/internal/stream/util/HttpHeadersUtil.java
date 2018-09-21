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
package org.reaktivity.nukleus.http_cache.internal.stream.util;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORITY;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.RETRY_AFTER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;

public final class HttpHeadersUtil
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    public static final Predicate<? super HttpHeaderFW> HAS_CACHE_CONTROL = h ->
    {
        String name = h.name().asString();
        return "cache-control".equalsIgnoreCase(name);
    };

    public static final Predicate<? super HttpHeaderFW> HAS_AUTHORIZATION = h ->
    {
        String name = h.name().asString();
        return AUTHORITY.equalsIgnoreCase(name);
    };

    public static final Predicate<? super HttpHeaderFW> HAS_RETRY_AFTER = h ->
    {
        String name = h.name().asString();
        return RETRY_AFTER.equalsIgnoreCase(name);
    };

    public static String getRequestURL(ListFW<HttpHeaderFW> headers)
    {
        // TODO, less garbage collection...
        // streaming API: https://github.com/reaktivity/nukleus-maven-plugin/issues/16
        final StringBuilder scheme = new StringBuilder();
        final StringBuilder path = new StringBuilder();
        final StringBuilder authority = new StringBuilder();
        headers.forEach(h ->
        {
            switch (h.name().asString())
            {
                case AUTHORITY:
                    authority.append(h.value().asString());
                    break;
                case HttpHeaders.PATH:
                    path.append(h.value().asString());
                    break;
                case HttpHeaders.SCHEME:
                    scheme.append(h.value().asString());
                    break;
                default:
                    break;
                }
        });
        return scheme.append(authority.toString()).append(path.toString()).toString();
    }

    public static String getHeader(ListFW<HttpHeaderFW> cachedRequestHeadersRO, String headerName)
    {
        // TODO remove GC when have streaming API: https://github.com/reaktivity/nukleus-maven-plugin/issues/16
        final StringBuilder header = new StringBuilder();
        cachedRequestHeadersRO.forEach(h ->
        {
            if (headerName.equalsIgnoreCase(h.name().asString()))
            {
                header.append(h.value().asString());
            }
        });

        return header.length() == 0 ? null : header.toString();
    }

    public static String getHeaderOrDefault(
            ListFW<HttpHeaderFW> responseHeaders,
            String headerName,
            String defaulted)
    {
        final String result = getHeader(responseHeaders, headerName);
        return result == null ? defaulted : result;
    }

    public static boolean hasStatusCode(
        ListFW<HttpHeaderFW> responseHeaders,
        int statusCode)
    {
        return  (responseHeaders.anyMatch(h ->
                STATUS.equals(h.name().asString()) && (Integer.toString(statusCode)).equals(h.value().asString())));
    }

    public static boolean retry(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        return hasStatusCode(responseHeaders, 503) && responseHeaders.anyMatch(HAS_RETRY_AFTER);
    }

    /*
     * Retry-After supports two formats. For example:
     * Retry-After: Wed, 21 Oct 2015 07:28:00 GMT
     * Retry-After: 120
     *
     * @return wait time in millis from now for both formats
     */
    public static long retryAfter(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        HttpHeaderFW header = responseHeaders.matchFirst(HAS_RETRY_AFTER);
        assert header != null;

        String retryAfter = header.value().asString();
        try
        {
            if (retryAfter != null && !retryAfter.isEmpty())
            {
                if (Character.isDigit(retryAfter.charAt(0)))
                {
                    return Integer.valueOf(retryAfter) * 1000;
                }
                else
                {
                    Date date = DATE_FORMAT.parse(retryAfter);
                    long wait = date.toInstant().toEpochMilli() - Instant.now().toEpochMilli();
                    return Math.max(wait, 0);
                }
            }
        }
        catch (Exception e)
        {
            // ignore
        }
        return 0L;
    }
}
