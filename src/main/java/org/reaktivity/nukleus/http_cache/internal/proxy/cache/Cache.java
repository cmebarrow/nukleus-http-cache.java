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

import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.InitialRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.PreferWaitIfNoneMatchRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request.Type;
import org.reaktivity.nukleus.http_cache.internal.stream.BudgetManager;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

public class Cache
{
    final Writer writer;
    final BudgetManager budgetManager;
    final Int2CacheHashMapWithLRUEviction cachedEntries;
    public final BufferPool cachedRequestBufferPool;
    public final BufferPool cachedResponseBufferPool;
    final BufferPool cachedRequest1BufferPool;
    final BufferPool cachedResponse1BufferPool;

    final BufferPool refreshBufferPool;
    final BufferPool requestBufferPool;
    final BufferPool responseBufferPool;

    final ListFW<HttpHeaderFW> cachedRequestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedRequest1HeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedResponse1HeadersRO = new HttpBeginExFW().headers();

    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> responseHeadersRO = new HttpBeginExFW().headers();

    final WindowFW windowRO = new WindowFW();

    static final String RESPONSE_IS_STALE = "110 - \"Response is Stale\"";

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();
    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<Request> correlations;
    final Supplier<String> etagSupplier;
    final Int2ObjectHashMap<PendingCacheEntries> uncommittedRequests = new Int2ObjectHashMap<>();
    final Int2ObjectHashMap<PendingInitialRequests> pendingInitialRequestsMap = new Int2ObjectHashMap<>();
    final HttpCacheCounters counters;

    public Cache(
        LongObjectBiConsumer<Runnable> scheduler,
        BudgetManager budgetManager,
        MutableDirectBuffer writeBuffer,
        BufferPool requestBufferPool,
        BufferPool cacheBufferPool,
        Long2ObjectHashMap<Request> correlations,
        Supplier<String> etagSupplier,
        HttpCacheCounters counters,
        LongConsumer entryCount)
    {
        this.scheduler = scheduler;
        this.budgetManager = budgetManager;
        this.correlations = correlations;
        this.writer = new Writer(writeBuffer);
        this.refreshBufferPool = new CountingBufferPool(
                requestBufferPool.duplicate(),
                counters.supplyCounter.apply("refresh.request.acquires"),
                counters.supplyCounter.apply("refresh.request.releases"));
        this.cachedRequestBufferPool = new CountingBufferPool(
                cacheBufferPool,
                counters.supplyCounter.apply("cached.request.acquires"),
                counters.supplyCounter.apply("cached.request.releases"));
        this.cachedResponseBufferPool = new CountingBufferPool(
                cacheBufferPool.duplicate(),
                counters.supplyCounter.apply("cached.response.acquires"),
                counters.supplyCounter.apply("cached.response.releases"));
        this.cachedRequest1BufferPool = cacheBufferPool.duplicate();
        this.cachedResponse1BufferPool = cacheBufferPool.duplicate();
        this.requestBufferPool = requestBufferPool.duplicate();
        this.responseBufferPool = requestBufferPool.duplicate();
        this.cachedEntries = new Int2CacheHashMapWithLRUEviction(entryCount);
        this.etagSupplier = etagSupplier;
        this.counters = counters;
    }

    public void put(
        int requestUrlHash,
        CacheableRequest request)
    {
        CacheEntry oldCacheEntry = cachedEntries.get(requestUrlHash);
        if (oldCacheEntry == null)
        {
            CacheEntry cacheEntry = new CacheEntry(
                    this,
                    request,
                    true);
            updateCache(requestUrlHash, cacheEntry);
        }
        else
        {
            boolean expectSubscribers = request.getType() == Type.INITIAL_REQUEST ? true: oldCacheEntry.expectSubscribers();
            CacheEntry cacheEntry = new CacheEntry(
                    this,
                    request,
                    expectSubscribers);

            if (cacheEntry.isIntendedForSingleUser())
            {
                cacheEntry.purge();
            }
            else if (oldCacheEntry.isUpdatedBy(request))
            {
                updateCache(requestUrlHash, cacheEntry);

                boolean notVaries = oldCacheEntry.doesNotVaryBy(cacheEntry);
                if (notVaries)
                {
                    oldCacheEntry.subscribers(cacheEntry::serveClient);
                }
                else
                {
                    oldCacheEntry.subscribers(subscriber ->
                    {
                        final MessageConsumer acceptReply = subscriber.acceptReply();
                        final long acceptReplyStreamId = subscriber.acceptReplyStreamId();
                        final long acceptCorrelationId = subscriber.acceptCorrelationId();
                        this.writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);

                        // count all responses
                        counters.responses.getAsLong();

                        // count ABORTed responses
                        counters.responsesAbortedVary.getAsLong();
                    });
                }

                oldCacheEntry.purge();
            }
            else
            {
                cacheEntry.purge();
                if (request.getType() == Request.Type.CACHE_REFRESH)
                {
                    oldCacheEntry.refresh(request);
                }
            }
        }
    }

    private void updateCache(
            int requestUrlHash,
            CacheEntry cacheEntry)
    {
        cacheEntry.commit();
        cachedEntries.put(requestUrlHash, cacheEntry);
        PendingCacheEntries result = this.uncommittedRequests.remove(requestUrlHash);
        if (result != null)
        {
            result.addSubscribers(cacheEntry);
        }
    }

    public boolean handleInitialRequest(
            int requestURLHash,
            ListFW<HttpHeaderFW> request,
            short authScope,
            CacheableRequest cacheableRequest)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        if (cacheEntry != null)
        {
            return serveRequest(cacheEntry, request, authScope, cacheableRequest);
        }
        else
        {
            return false;
        }
    }

    public void servePendingInitialRequests(
        int requestURLHash)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.remove(requestURLHash);
        if (pendingInitialRequests != null)
        {
            pendingInitialRequests.removeSubscribers(s ->
            {
                boolean served = false;

                if (cacheEntry != null)
                {
                    served = serveRequest(cacheEntry, s.getRequestHeaders(requestHeadersRO),
                            s.authScope(), s);
                }

                if (!served)
                {
                    sendPendingInitialRequest(s);
                }
            });
        }
    }

    public void sendPendingInitialRequests(
        int requestURLHash)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.remove(requestURLHash);
        if (pendingInitialRequests != null)
        {
            pendingInitialRequests.removeSubscribers(this::sendPendingInitialRequest);
        }
    }

    private void sendPendingInitialRequest(
        final InitialRequest request)
    {
        long connectStreamId = request.supplyStreamId().getAsLong();
        long connectCorrelationId = request.supplyCorrelationId().getAsLong();
        ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(requestHeadersRO);

        correlations.put(connectCorrelationId, request);

        writer.doHttpRequest(request.connect(), connectStreamId, request.connectRef(), connectCorrelationId,
                builder -> requestHeaders.forEach(
                        h ->  builder.item(item -> item.name(h.name()).value(h.value()))
                )
        );
        writer.doHttpEnd(request.connect(), connectStreamId);
    }

    public boolean hasPendingInitialRequests(
        int requestURLHash)
    {
        return pendingInitialRequestsMap.containsKey(requestURLHash);
    }

    public void addPendingRequest(
        InitialRequest initialRequest)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.get(initialRequest.requestURLHash());
        pendingInitialRequests.subscribe(initialRequest);
    }

    public void createPendingInitialRequests(
        InitialRequest initialRequest)
    {
        pendingInitialRequestsMap.put(initialRequest.requestURLHash(), new PendingInitialRequests(initialRequest));
    }

    public void handlePreferWaitIfNoneMatchRequest(
        int requestURLHash,
        PreferWaitIfNoneMatchRequest preferWaitRequest,
        ListFW<HttpHeaderFW> requestHeaders,
        short authScope)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        PendingCacheEntries uncommittedRequest = this.uncommittedRequests.get(requestURLHash);

        String ifNoneMatch = HttpHeadersUtil.getHeader(requestHeaders, HttpHeaders.IF_NONE_MATCH);
        assert ifNoneMatch != null;
        if (uncommittedRequest != null && ifNoneMatch.contains(uncommittedRequest.etag())
                && doesNotVary(requestHeaders, uncommittedRequest.request))
        {
            uncommittedRequest.subscribe(preferWaitRequest);
        }
        else if (cacheEntry == null)
        {
            final MessageConsumer acceptReply = preferWaitRequest.acceptReply();
            final long acceptReplyStreamId = preferWaitRequest.acceptReplyStreamId();
            final long acceptCorrelationId = preferWaitRequest.acceptCorrelationId();
            writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);

            // count all responses
            counters.responses.getAsLong();

            // count ABORTed responses
            counters.responsesAbortedEvicted.getAsLong();
        }
        else if (cacheEntry.isUpdateRequestForThisEntry(requestHeaders))
        {
            // TODO return value ??
            cacheEntry.subscribeWhenNoneMatch(preferWaitRequest);
        }
        else if (cacheEntry.canServeUpdateRequest(requestHeaders))
        {
            cacheEntry.serveClient(preferWaitRequest);
        }
        else
        {
            final MessageConsumer acceptReply = preferWaitRequest.acceptReply();
            final long acceptReplyStreamId = preferWaitRequest.acceptReplyStreamId();
            final long acceptCorrelationId = preferWaitRequest.acceptCorrelationId();

            writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);

            // count all responses
            counters.responses.getAsLong();

            // count ABORTed responses
            counters.responsesAbortedMiss.getAsLong();
        }
    }

    private boolean doesNotVary(
            ListFW<HttpHeaderFW> requestHeaders,
            InitialRequest request)
    {
        ListFW<HttpHeaderFW> cachedRequestHeaders = request.getRequestHeaders(cachedRequestHeadersRO);
        ListFW<HttpHeaderFW> cachedResponseHeaders = request.getResponseHeaders(cachedResponseHeadersRO);
        return CacheUtils.doesNotVary(requestHeaders, cachedResponseHeaders, cachedRequestHeaders);
    }

    private boolean serveRequest(
            CacheEntry entry,
            ListFW<HttpHeaderFW> request,
            short authScope,
            AnswerableByCacheRequest cacheableRequest)
    {
        if (entry.canServeRequest(request, authScope))
        {
            final String requestAuthorizationHeader = getHeader(request, AUTHORIZATION);
            entry.recentAuthorizationHeader(requestAuthorizationHeader);

            boolean etagMatched = CacheUtils.isMatchByEtag(request, entry.cachedRequest.etag());
            if (etagMatched)
            {
                send304(entry, cacheableRequest);
            }
            else
            {
                entry.serveClient(cacheableRequest);
            }

            return true;
        }
        return false;
    }

    private void send304(CacheEntry entry, AnswerableByCacheRequest request)
    {
        writer.doHttpResponse(request.acceptReply(), request.acceptReplyStreamId(), 0L, request.acceptCorrelationId(), e ->
                e.item(h -> h.name(STATUS).value("304"))
                 .item(h -> h.name(ETAG).value(entry.cachedRequest.etag())));
        writer.doHttpEnd(request.acceptReply(), request.acceptReplyStreamId());

        request.purge();

        // count all responses
        counters.responses.getAsLong();
    }

    public void notifyUncommitted(InitialRequest request)
    {
        this.uncommittedRequests.computeIfAbsent(request.requestURLHash(), p -> new PendingCacheEntries(request));
    }

    public void removeUncommitted(InitialRequest request)
    {
        this.uncommittedRequests.computeIfPresent(request.requestURLHash(), (k, v) ->
        {
            v.removeSubscribers(subscriber ->
            {
                final MessageConsumer acceptReply = subscriber.acceptReply();
                final long acceptReplyStreamId = subscriber.acceptReplyStreamId();
                final long acceptCorrelationId = subscriber.acceptCorrelationId();
                this.writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);

                // count all responses
                counters.responses.getAsLong();

                // count ABORTed responses
                counters.responsesAbortedUncommited.getAsLong();
            });
            return null;
        });
    }

    public void purge(CacheEntry entry)
    {
        this.cachedEntries.remove(entry.requestUrl());
        entry.purge();
    }

    public boolean purgeOld()
    {
        return this.cachedEntries.purgeLRU();
    }

}
