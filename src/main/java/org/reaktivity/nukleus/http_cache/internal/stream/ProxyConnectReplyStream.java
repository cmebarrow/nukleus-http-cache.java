/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.nukleus.http_cache.internal.stream;

import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.SurrogateControl;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheRefreshRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class ProxyConnectReplyStream
{
    private final ProxyStreamFactory streamFactory;

    private MessageConsumer streamState;

    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyStreamId;

    private Request streamCorrelation;

    private int acceptReplyBudget;
    private int connectReplyBudget;
    private long groupId;
    private int padding;
    private boolean endDeferred;
    private boolean cached;

    ProxyConnectReplyStream(
            ProxyStreamFactory proxyStreamFactory,
            MessageConsumer connectReplyThrottle,
            long connectReplyId)
    {
        this.streamFactory = proxyStreamFactory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectReplyStreamId = connectReplyId;
        this.streamState = this::beforeBegin;
    }

    void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = this.streamFactory.beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
        }
        else
        {
            this.streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
        }
    }

    private void handleBegin(
        BeginFW begin)
    {
        final long connectReplyRef = begin.sourceRef();
        final long connectCorrelationId = begin.correlationId();

        this.streamCorrelation = connectReplyRef == 0L ?
            this.streamFactory.correlations.remove(connectCorrelationId) : null;
        final OctetsFW extension = streamFactory.beginRO.extension();

        if (streamCorrelation != null && extension.sizeof() > 0)
        {
            final HttpBeginExFW httpBeginFW = extension.get(streamFactory.httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

            switch(streamCorrelation.getType())
            {
                case PROXY:
                    doProxyBegin(responseHeaders);
                    break;
                case INITIAL_REQUEST:
                    handleInitialRequest(responseHeaders);
                    break;
                case CACHE_REFRESH:
                    handleCacheRefresh(responseHeaders);
                    break;
                default:
                    throw new RuntimeException("Not implemented");
            }
        }
        else
        {
            this.streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
        }
    }

    ///////////// CACHE REFRESH
    private void handleCacheRefresh(
            ListFW<HttpHeaderFW> responseHeaders)
    {
        boolean retry = HttpHeadersUtil.retry(responseHeaders);
        if (retry && ((CacheableRequest)streamCorrelation).attempts() < 3)
        {
            retryCacheableRequest();
            return;
        }
        CacheRefreshRequest request = (CacheRefreshRequest) this.streamCorrelation;
        if (request.storeResponseHeaders(responseHeaders, streamFactory.cache, streamFactory.responseBufferPool))
        {
            this.streamState = this::handleCacheRefresh;
            streamFactory.writer.doWindow(connectReplyThrottle, connectReplyStreamId, 32767, 0, 0L);
        }
        else
        {
            request.purge();
            this.streamState = this::reset;
            streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
        }
    }

    private void reset(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        // NOOP
    }

    private void handleCacheRefresh(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;

        switch (msgTypeId)
        {
            case DataFW.TYPE_ID:
                final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
                boolean stored = request.storeResponseData(this.streamFactory.cache, data, streamFactory.responseBufferPool);
                if (!stored)
                {
                    request.purge();
                    this.streamState = this::reset;
                    streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
                }
                else
                {
                    streamFactory.writer.doWindow(connectReplyThrottle, connectReplyStreamId, length, 0, 0L);
                }
                break;
            case EndFW.TYPE_ID:
                final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
                request.cache(end, streamFactory.cache);
                break;
            case AbortFW.TYPE_ID:
            default:
                request.purge();
                break;
        }
    }

    ///////////// INITIAL_REQUEST REQUEST
    private void handleInitialRequest(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        boolean retry = HttpHeadersUtil.retry(responseHeaders);
        if (retry && ((CacheableRequest)streamCorrelation).attempts() < 3)
        {
            retryCacheableRequest();
            return;
        }
        int freshnessExtension = SurrogateControl.getSurrogateFreshnessExtension(responseHeaders);
        final boolean isCacheableResponse = isCacheableResponse(responseHeaders);

        if (freshnessExtension > 0 && isCacheableResponse)
        {
            handleEdgeArchSync(responseHeaders, freshnessExtension);
        }
        else if(isCacheableResponse)
        {
            handleCacheableResponse(responseHeaders);
        }
        else
        {
            streamCorrelation.purge();
            doProxyBegin(responseHeaders);
        }
    }

    private void handleEdgeArchSync(
        ListFW<HttpHeaderFW> responseHeaders,
        int freshnessExtension)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;

        if (request.storeResponseHeaders(responseHeaders, streamFactory.cache, streamFactory.responseBufferPool))
        {
            final MessageConsumer acceptReply = streamCorrelation.acceptReply();
            final long acceptReplyStreamId = streamCorrelation.acceptReplyStreamId();
            final long acceptReplyRef = streamCorrelation.acceptRef();
            final long correlationId = streamCorrelation.acceptCorrelationId();

            streamCorrelation.setThrottle(this::handleProxyThrottle);
            streamFactory.writer.doHttpResponseWithUpdatedCacheControl(
                    acceptReply,
                    acceptReplyStreamId,
                    acceptReplyRef,
                    correlationId,
                    streamFactory.cacheControlParser,
                    responseHeaders,
                    freshnessExtension,
                    request.etag(),
                    false
                    );

            // count all responses
            streamFactory.counters.responses.getAsLong();

            streamFactory.writer.doHttpPushPromise(request, request, responseHeaders, freshnessExtension, request.etag());

            // count all promises (prefer wait, if-none-match)
            streamFactory.counters.promises.getAsLong();

            this.streamState = this::handleCacheableRequestResponse;
        }
        else
        {
            request.purge();
            doProxyBegin(responseHeaders);
        }
    }

    private void retryCacheableRequest()
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;
        request.incAttempts();

        MessageConsumer connect = request.connect();
        long connectStreamId = request.supplyStreamId().getAsLong();
        long connectRef = request.connectRef();
        long connectCorrelationId = request.supplyCorrelationId().getAsLong();

        streamFactory.correlations.put(connectCorrelationId, request);
        ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(streamFactory.requestHeadersRO);
        final String etag = request.etag();
        streamFactory.writer.doHttpRequest(connect, connectStreamId, connectRef, connectCorrelationId,
                builder ->
                {
                    requestHeaders.forEach(
                            h ->  builder.item(item -> item.name(h.name()).value(h.value())));
                    if (request instanceof CacheRefreshRequest)
                    {
                        builder.item(item -> item.name(HttpHeaders.IF_NONE_MATCH).value(etag));
                    }
                });
        streamFactory.writer.doHttpEnd(connect, connectStreamId);
        streamFactory.counters.requestsRetry.getAsLong();
    }

    private void handleCacheableResponse(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;
        if (!request.storeResponseHeaders(responseHeaders, streamFactory.cache, streamFactory.responseBufferPool))
        {
            request.purge();
        }
        doProxyBegin(responseHeaders);
        this.streamState = this::handleCacheableRequestResponse;
    }

    private void handleCacheableRequestResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;

        switch (msgTypeId)
        {
            case DataFW.TYPE_ID:
                final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
                boolean stored = request.storeResponseData(streamFactory.cache, data, streamFactory.responseBufferPool);
                if (!stored)
                {
                    request.purge();
                }
                break;
            case EndFW.TYPE_ID:
                final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
                request.cache(end, streamFactory.cache);
                cached = true;
                break;
            case AbortFW.TYPE_ID:
            default:
                request.purge();
                break;
        }
        this.handleFramesWhenProxying(msgTypeId, buffer, index, length);
    }

    ///////////// PROXY
    private void doProxyBegin(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyStreamId();
        final long acceptReplyRef = streamCorrelation.acceptRef();
        final long correlationId = streamCorrelation.acceptCorrelationId();

        streamCorrelation.setThrottle(this::handleProxyThrottle);
        streamFactory.writer.doHttpResponse(
                acceptReply,
                acceptReplyStreamId,
                acceptReplyRef,
                correlationId,
                builder -> responseHeaders.forEach(
                        h -> builder.item(item -> item.name(h.name()).value(h.value()))
            ));

        // count all responses
        streamFactory.counters.responses.getAsLong();

        this.streamState = this::handleFramesWhenProxying;
    }

    private void handleFramesWhenProxying(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyStreamId();
        switch (msgTypeId)
        {
            case DataFW.TYPE_ID:
                final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
                connectReplyBudget -= data.length() + data.padding();
                if (connectReplyBudget < 0)
                {
                    streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
                }
                else
                {
                    final OctetsFW payload = data.payload();
                    acceptReplyBudget -= payload.sizeof() + data.padding();
                    assert acceptReplyBudget >= 0;
                    streamFactory.writer.doHttpData(
                            acceptReply,
                            acceptReplyStreamId,
                            data.groupId(),
                            data.padding(),
                            payload.buffer(),
                            payload.offset(),
                            payload.sizeof());
                }
                break;
            case EndFW.TYPE_ID:
                streamFactory.budgetManager.closing(groupId, acceptReplyStreamId, connectReplyBudget);
                if (streamFactory.budgetManager.hasUnackedBudget(groupId, acceptReplyStreamId))
                {
                    endDeferred = true;
                }
                else
                {
                    streamFactory.budgetManager.closed(BudgetManager.StreamKind.PROXY, groupId, acceptReplyStreamId);
                    streamFactory.writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                }
                break;
            case AbortFW.TYPE_ID:
                streamFactory.budgetManager.closed(BudgetManager.StreamKind.PROXY, groupId, acceptReplyStreamId);
                streamFactory.writer.doAbort(acceptReply, acceptReplyStreamId);
                break;
            default:
                streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
                break;
            }
    }

    private void handleProxyThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        final long acceptReplyStreamId = streamCorrelation.acceptReplyStreamId();

        switch (msgTypeId)
        {
            case WindowFW.TYPE_ID:
                final WindowFW window = streamFactory.windowRO.wrap(buffer, index, index + length);
                final long streamId = window.streamId();
                final int credit = window.credit();
                acceptReplyBudget += credit;
                padding = window.padding();
                groupId = window.groupId();
                streamFactory.budgetManager.window(BudgetManager.StreamKind.PROXY, groupId, streamId, credit, this::sendWindow);
                if (endDeferred && !streamFactory.budgetManager.hasUnackedBudget(groupId, streamId))
                {
                    final MessageConsumer acceptReply = streamCorrelation.acceptReply();
                    streamFactory.budgetManager.closed(BudgetManager.StreamKind.PROXY, groupId, acceptReplyStreamId);
                    streamFactory.writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                }
                break;
            case ResetFW.TYPE_ID:
                streamFactory.budgetManager.closed(BudgetManager.StreamKind.PROXY, groupId, acceptReplyStreamId);
                streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
                // if cached, do not purge the buffer slots as it may be used by other clients
                if (!cached)
                {
                    streamCorrelation.purge();
                }
                break;
            default:
                // TODO,  ABORT and RESET
                break;
            }
    }

    private int sendWindow(int credit)
    {
        if (endDeferred)
        {
            return credit;
        }
        else
        {
            connectReplyBudget += credit;
            streamFactory.writer.doWindow(connectReplyThrottle, connectReplyStreamId, credit, padding, groupId);
            return 0;
        }
    }

}
