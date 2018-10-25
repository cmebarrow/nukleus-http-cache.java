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
package org.reaktivity.nukleus.http_cache.internal.bench;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(3)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
public class HttpPushServerBM
{
//    private final Configuration configuration;
//    private final Reaktor reaktor;
//    private HttpPushController controller;
//
//    {
//        Properties properties = new Properties();
//        properties.setProperty(DIRECTORY_PROPERTY_NAME, "target/nukleus-benchmarks");
//        properties.setProperty(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, Long.toString(1024L * 1024L * 16L));
//
//        this.configuration = new Configuration(properties);
//
//        try
//        {
//            Files.walk(configuration.directory(), FOLLOW_LINKS)
//                 .map(Path::toFile)
//                 .forEach(File::delete);
//        }
//        catch (IOException ex)
//        {
//            LangUtil.rethrowUnchecked(ex);
//        }
//
//        this.reaktor = Reaktor.builder()
//                              .config(configuration)
//                              .nukleus("http-cache"::equals)
//                              .controller(HttpPushController.class::isAssignableFrom)
//                              .errorHandler(ex -> ex.printStackTrace(System.err))
//                              .build();
//
//        this.controller = reaktor.controller(HttpPushController.class);
//    }
//
//    private final BeginFW beginRO = new BeginFW();
//    private final DataFW dataRO = new DataFW();
//
//    private final BeginFW.Builder beginRW = new BeginFW.Builder();
//    private final DataFW.Builder dataRW = new DataFW.Builder();
//    private final WindowFW.Builder windowRW = new WindowFW.Builder();
//
//    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
//
//    private HttpPushStreams sourceInputStreams;
//    private HttpPushStreams sourceOutputEstStreams;
//
//    private MutableDirectBuffer throttleBuffer;
//
//    private long sourceInputRef;
//    private long targetInputRef;
//
//    private long sourceInputId;
//    private DataFW data;
//
//    private MessageHandler sourceOutputEstHandler;
//
//    @Setup(Level.Trial)
//    public void reinit() throws Exception
//    {
//        final Random random = new Random();
//        final HttpPushController controller = reaktor.controller(HttpPushController.class);
//
//        this.targetInputRef = random.nextLong();
//        this.sourceInputRef = controller.routeProxy("source", 0L, "target", targetInputRef).get();
//
//        this.sourceInputStreams = controller.streams("source");
//        this.sourceOutputEstStreams = controller.streams("http-cache", "target");
//
//        this.sourceInputId = random.nextLong();
//        this.sourceOutputEstHandler = this::processBegin;
//
//        final Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers = hs ->
//        {
//            hs.item(h -> h.name(":scheme").value("http"));
//            hs.item(h -> h.name(":method").value("GET"));
//            hs.item(h -> h.name(":path").value("/"));
//            hs.item(h -> h.name("host").value("localhost:8080"));
//            hs.item(h -> h.name("upgrade").value("websocket"));
//            hs.item(h -> h.name("sec-websocket-key").value("dGhlIHNhbXBsZSBub25jZQ=="));
//            hs.item(h -> h.name("sec-websocket-version").value("13"));
//
////            hs.item(h -> h.name("sec-websocket-protocol").value(protocol));
//        };
//
//        final AtomicBuffer writeBuffer = new UnsafeBuffer(new byte[256]);
//
//        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
//                .streamId(sourceInputId)
//                .source(HttpPushNukleus.NAME)
//                .sourceRef(sourceInputRef)
//                .correlationId(random.nextLong())
//                .build();
//
//        this.sourceInputStreams.writeStreams(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
//
//        byte[] charBytes = "Hello, world".getBytes(StandardCharsets.UTF_8);
//
//        byte[] sendArray = new byte[18];
//        sendArray[0] = (byte) 0x82; // fin, binary
//        sendArray[1] = (byte) 0x8c; // masked, length 12
//        sendArray[2] = (byte) 0x01; // masking key (4 bytes)
//        sendArray[3] = (byte) 0x02;
//        sendArray[4] = (byte) 0x03;
//        sendArray[5] = (byte) 0x04;
//        sendArray[6] = (byte) (charBytes[0] ^ sendArray[2]);
//        sendArray[7] = (byte) (charBytes[1] ^ sendArray[3]);
//        sendArray[8] = (byte) (charBytes[2] ^ sendArray[4]);
//        sendArray[9] = (byte) (charBytes[3] ^ sendArray[5]);
//        sendArray[10] = (byte) (charBytes[4] ^ sendArray[2]);
//        sendArray[11] = (byte) (charBytes[5] ^ sendArray[3]);
//        sendArray[12] = (byte) (charBytes[6] ^ sendArray[4]);
//        sendArray[13] = (byte) (charBytes[7] ^ sendArray[5]);
//        sendArray[14] = (byte) (charBytes[8] ^ sendArray[2]);
//        sendArray[15] = (byte) (charBytes[9] ^ sendArray[3]);
//        sendArray[16] = (byte) (charBytes[10] ^ sendArray[4]);
//        sendArray[17] = (byte) (charBytes[11] ^ sendArray[5]);
//
//        this.data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
//                          .streamId(sourceInputId)
//                          .payload(p -> p.set(sendArray))
//                          .build();
//
//        this.throttleBuffer = new UnsafeBuffer(allocateDirect(SIZE_OF_LONG + SIZE_OF_INT));
//    }
//
//    @TearDown(Level.Trial)
//    public void reset() throws Exception
//    {
//        HttpPushController controller = reaktor.controller(HttpPushController.class);
//
//        controller.unrouteProxy("source", sourceInputRef, "target", targetInputRef).get();
//
//        this.sourceInputStreams.close();
//        this.sourceInputStreams = null;
//
//        this.sourceOutputEstStreams.close();
//        this.sourceOutputEstStreams = null;
//    }
//
//    @Benchmark
//    @Group("throughput")
//    @GroupThreads(1)
//    public void writer(Control control) throws Exception
//    {
//        while (!control.stopMeasurement &&
//               !sourceInputStreams.writeStreams(data.typeId(), data.buffer(), 0, data.limit()))
//        {
//            Thread.yield();
//        }
//
//        while (!control.stopMeasurement &&
//                sourceInputStreams.readThrottle((t, b, o, l) -> {}) == 0)
//        {
//            Thread.yield();
//        }
//    }
//
//    @Benchmark
//    @Group("throughput")
//    @GroupThreads(1)
//    public void reader(Control control) throws Exception
//    {
//        while (!control.stopMeasurement &&
//               sourceOutputEstStreams.readStreams(this::handleReply) == 0)
//        {
//            Thread.yield();
//        }
//    }
//
//    private void handleReply(
//        int msgTypeId,
//        MutableDirectBuffer buffer,
//        int index,
//        int length)
//    {
//        sourceOutputEstHandler.onMessage(msgTypeId, buffer, index, length);
//    }
//
//    private void processBegin(
//        int msgTypeId,
//        MutableDirectBuffer buffer,
//        int index,
//        int length)
//    {
//        beginRO.wrap(buffer, index, index + length);
//        final long streamId = beginRO.streamId();
//        doWindow(streamId, 8192);
//
//        this.sourceOutputEstHandler = this::processData;
//    }
//
//    private void processData(
//        int msgTypeId,
//        MutableDirectBuffer buffer,
//        int index,
//        int length)
//    {
//        dataRO.wrap(buffer, index, index + length);
//        final long streamId = dataRO.streamId();
//        final OctetsFW payload = dataRO.payload();
//
//        final int update = payload.sizeof();
//        doWindow(streamId, update);
//    }
//
//    private void doWindow(
//        final long streamId,
//        final int update)
//    {
//        final WindowFW window = windowRW.wrap(throttleBuffer, 0, throttleBuffer.capacity())
//                .streamId(streamId)
//                .update(update)
//                .build();
//
//        sourceOutputEstStreams.writeThrottle(window.typeId(), window.buffer(), window.offset(), window.sizeof());
//    }
//
//    private Flyweight.Builder.Visitor visitHttpBeginEx(
//        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
//    {
//        return (buffer, offset, limit) ->
//            httpBeginExRW.wrap(buffer, offset, limit)
//                         .headers(headers)
//                         .build()
//                         .sizeof();
//    }
//
//    public static void main(String[] args) throws RunnerException
//    {
//        Options opt = new OptionsBuilder()
//                .include(HttpPushServerBM.class.getSimpleName())
//                .forks(0)
//                .build();
//
//        new Runner(opt).run();
//    }
}
