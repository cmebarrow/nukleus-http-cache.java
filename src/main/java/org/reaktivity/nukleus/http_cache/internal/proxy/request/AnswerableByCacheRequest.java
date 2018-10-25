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
package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

public abstract class AnswerableByCacheRequest extends Request
{
    private final int requestURLHash;
    private final long authorization;
    private final short authScope;
    private final boolean authorizationHeader;
    private String etag;

    public AnswerableByCacheRequest(
            String acceptName,
            MessageConsumer acceptReply,
            long acceptReplyStreamId,
            long acceptCorrelationId,
            RouteManager router,
            int requestURLHash,
            boolean authorizationHeader,
            long authorization,
            short authScope,
            String etag)
    {
        super(acceptName, acceptReply, acceptReplyStreamId, acceptCorrelationId, router);
        this.requestURLHash = requestURLHash;
        this.authorizationHeader = authorizationHeader;
        this.authorization = authorization;
        this.authScope = authScope;
        this.etag = etag;
    }

    public final boolean authorizationHeader()
    {
        return authorizationHeader;
    }

    public final long authorization()
    {
        return authorization;
    }

    public final short authScope()
    {
        return authScope;
    }

    public final String etag()
    {
        return etag;
    }

    public final int requestURLHash()
    {
        return requestURLHash;
    }

    protected void etag(String etag)
    {
        this.etag = etag;
    }

}