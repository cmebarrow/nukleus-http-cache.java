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

import java.util.Optional;
import java.util.function.LongSupplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.OnUpdateRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public class Cache
{

    final Writer writer;
    final Long2ObjectHashMap<CacheEntry> requestURLToResponse;
    final BufferPool requestBufferPool;
    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    final BufferPool responseBufferPool;
    final ListFW<HttpHeaderFW> responseHeadersRO = new HttpBeginExFW().headers();
    final LongSupplier streamSupplier;
    final LongSupplier supplyCorrelationId;
    final WindowFW windowRO = new WindowFW();
    static final String RESPONSE_IS_STALE = "110 - \"Response is Stale\"";

    final CacheControl responseCacheControlParser = new CacheControl();
    final CacheControl requestCacheControlParser = new CacheControl();

    public Cache(
            MutableDirectBuffer writeBuffer,
            LongSupplier streamSupplier,
            LongSupplier supplyCorrelationId,
            BufferPool bufferPool,
            Long2ObjectHashMap<Request> correlations)
    {
        this.streamSupplier = streamSupplier;
        this.supplyCorrelationId = supplyCorrelationId;
        this.writer = new Writer(writeBuffer);
        this.requestBufferPool = bufferPool;
        this.responseBufferPool = bufferPool.duplicate();
        this.requestURLToResponse = new Long2ObjectHashMap<>();
    }

    public void put(
        int requestURLHash,
        int requestSlot,
        int requestSize,
        int responseSlot,
        int responseHeaderSize,
        int responseSize,
        short authScope)
    {
        CacheEntry responseServer = new CacheEntry(
                this,
                requestSlot,
                requestSize,
                responseSlot,
                responseHeaderSize,
                responseSize,
                authScope);

        CacheEntry oldCacheEntry = requestURLToResponse.put(requestURLHash, responseServer);
        if (oldCacheEntry != null)
        {
            oldCacheEntry.cleanUp();
        }
    }

    public Optional<CacheEntry> getResponseThatSatisfies(
            int requestURLHash,
            ListFW<HttpHeaderFW> request,
            short authScope)
    {
        // DPW TODO lookup if revalidating
        boolean isRevalidating = false;
        // Will be stream of responses in near future, so coding it as now.
        final CacheEntry cacheEntry = requestURLToResponse.get(requestURLHash);

        if (cacheEntry != null && cacheEntry.canServeRequest(requestURLHash, request, isRevalidating, authScope))
        {
            return Optional.of(cacheEntry);
        }
        else
        {
            return Optional.empty();
        }
    }

    public void onUpdate(OnUpdateRequest onModificationRequest)
    {
        throw new RuntimeException("DPW to implement");
    }

}
