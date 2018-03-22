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

import java.util.List;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;

public class Int2CacheHashMapWithLRUEviction
{

    private static final int PURGE_SIZE = 1;
    private final Int2ObjectHashMap<CacheEntry> cachedEntries;
    private final IntArrayList lruEntryList;

    public Int2CacheHashMapWithLRUEviction()
    {
        cachedEntries = new Int2ObjectHashMap<>();
        lruEntryList = new IntArrayList();
    }

    public void put(
        int requestUrlHash,
        CacheEntry cacheEntry)
    {
        cachedEntries.put(requestUrlHash, cacheEntry);
        lruEntryList.removeInt(requestUrlHash);
        lruEntryList.add(requestUrlHash);
    }

    public CacheEntry get(int requestUrlHash)
    {
        final CacheEntry result = cachedEntries.get(requestUrlHash);
        if (result != null)
        {
            lruEntryList.removeInt(requestUrlHash);
            lruEntryList.add(requestUrlHash);
        }
        return result;
    }

    public CacheEntry remove(int requestUrlHash)
    {
        final CacheEntry result = cachedEntries.remove(requestUrlHash);
        if (result != null)
        {
            lruEntryList.removeInt(requestUrlHash);
        }
        return result;
    }

    public void purgeLRU()
    {
        final List<Integer> subList = lruEntryList.subList(0, PURGE_SIZE);
        subList.stream().forEach(i ->
        {
            CacheEntry rm = cachedEntries.remove(i);
            assert rm != null;
            rm.purge();
        });
        subList.clear();
    }
}
