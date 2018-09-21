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
package org.reaktivity.nukleus.http_cache.internal;

import org.reaktivity.nukleus.Configuration;

public class HttpCacheConfiguration extends Configuration
{
    public static final String HTTP_CACHE_CAPACITY = "nukleus.http_cache.capacity";
    public static final String HTTP_CACHE_SLOT_CAPACITY = "nukleus.http_cache.slot.capacity";

    public static final String HTTP_CACHE_MAXIMUM_REQUESTS = "nukleus.http_cache.maximum.requests";

    private static final int HTTP_CACHE_CAPACITY_DEFAULT = 65536 * 64;
    private static final int HTTP_CACHE_SLOT_CAPACITY_DEFAULT = 0x4000; // ALSO is max header size
    private static final int HTTP_CACHE_MAXIMUM_REQUESTS_DEFAULT = 65536;

    public HttpCacheConfiguration(
        Configuration config)
    {
        super(config);
    }

    public int cacheCapacity()
    {
        return getInteger(HTTP_CACHE_CAPACITY, HTTP_CACHE_CAPACITY_DEFAULT);
    }

    public int cacheSlotCapacity()
    {
        return getInteger(HTTP_CACHE_SLOT_CAPACITY, HTTP_CACHE_SLOT_CAPACITY_DEFAULT);
    }

    public int maximumRequests()
    {
        return getInteger(HTTP_CACHE_MAXIMUM_REQUESTS, HTTP_CACHE_MAXIMUM_REQUESTS_DEFAULT);
    }

}
