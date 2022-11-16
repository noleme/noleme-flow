package com.noleme.flow;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.generator.IntegerGenerator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.Transformer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.noleme.flow.Flow.nonFatal;

/**
 * @author Pierre LECERF (pierre@noleme.com)
 * Created on 09/11/2022
 */
public class FlowTest
{
    @Test
    public void testStatic_pipe()
    {
        var flow = Flow.from(() -> 3);
        var add = Flow.pipe(flow, i -> i + 1);
        var mult = Flow.pipe(add, i -> i * 2);
        var result = mult.collect();

        int value = Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(flow).get(result));
        Assertions.assertEquals(8, value);
    }

    @Test
    public void testStatic_into_pipe()
    {
        var pipeFlow = Flow.from(() -> 3);
        var add = Flow.into(pipeFlow, (Transformer<Integer, Integer>) i -> i + 1);
        var mult = Flow.into(add, (Transformer<Integer, Integer>) i -> i * 2);
        var result = mult.collect();

        int pipeValue = Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(pipeFlow).get(result));
        Assertions.assertEquals(8, pipeValue);
    }

    @Test
    public void testStatic_into_sink()
    {
        var sinkProbe = new AtomicInteger(0);
        var sinkFlow = Flow.from(() -> 3);
        var sink = Flow.into(sinkFlow, (Loader<Integer>) sinkProbe::addAndGet);

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(sinkFlow));
        Assertions.assertEquals(3, sinkProbe.get());
    }

    @Test
    public void testStatic_into_stream()
    {
        var sinkProbe = new AtomicInteger(0);

        var streamFlow = Flow.from(() -> 3);
        var gen = Flow.into(streamFlow, (Function<Integer, Generator<Integer>>) max -> new IntegerGenerator(0, max));
        var add = Flow.into(gen, i -> i + 1);
        var sink = Flow.into(add, (Loader<Integer>) sinkProbe::addAndGet);

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(streamFlow));
        Assertions.assertEquals(6, sinkProbe.get());
    }

    @Test
    public void testNonFatal_extractor()
    {
        var probeSink = new AtomicInteger(0);
        var nfSource = Flow
            .from(nonFatal(() -> {
                throw new Exception();
            }))
            .sink(i -> probeSink.addAndGet(1))
        ;
        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfSource));
        Assertions.assertEquals(0, probeSink.get());
    }

    @Test
    public void testNonFatal_extractor_nonTriggered()
    {
        var probeSink = new AtomicInteger(0);
        var nfSource = Flow
            .from(nonFatal(() -> 1))
            .sink(probeSink::addAndGet)
        ;
        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfSource));
        Assertions.assertEquals(1, probeSink.get());
    }

    @Test
    public void testNonFatal_extractorWithConsumer()
    {
        var probeSink = new AtomicInteger(0);
        var probeConsumer = new AtomicInteger(0);
        var nfSource = Flow
            .from(nonFatal(() -> {
                throw new Exception();
            }, e -> probeConsumer.addAndGet(1)))
            .sink(i -> probeSink.addAndGet(1))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfSource));
        Assertions.assertEquals(0, probeSink.get());
        Assertions.assertEquals(1, probeConsumer.get());
    }

    @Test
    public void testNonFatal_transformer()
    {
        var probeSink = new AtomicInteger(0);
        var nfPipe = Flow
            .from(() -> 1)
            .pipe(nonFatal((Transformer<Integer, Integer>) i -> {
                throw new Exception();
            }))
            .sink(i -> probeSink.addAndGet(1))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfPipe));
        Assertions.assertEquals(0, probeSink.get());
    }

    @Test
    public void testNonFatal_transformer_nonTriggered()
    {
        var probeSink = new AtomicInteger(0);
        var nfPipe = Flow
            .from(() -> 1)
            .pipe(nonFatal((Transformer<Integer, Integer>) i -> i))
            .sink(probeSink::addAndGet)
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfPipe));
        Assertions.assertEquals(1, probeSink.get());
    }

    @Test
    public void testNonFatal_transformerWithConsumer()
    {
        var probeSink = new AtomicInteger(0);
        var probeConsumer = new AtomicInteger(0);
        var nfPipe = Flow
            .from(() -> 1)
            .pipe(nonFatal((Transformer<Integer, Integer>) i -> {
                throw new Exception();
            }, e -> probeConsumer.addAndGet(1)))
            .sink(i -> probeSink.addAndGet(1))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfPipe));
        Assertions.assertEquals(0, probeSink.get());
        Assertions.assertEquals(1, probeConsumer.get());
    }

    @Test
    public void testNonFatal_bitransformer()
    {
        var probeSink = new AtomicInteger(0);
        var nfPipe0 = Flow.from(() -> 1);
        var nfPipe1 = Flow
            .from(() -> 1)
            .join(nfPipe0, nonFatal((a, b) -> {
                throw new Exception();
            }))
            .sink(i -> probeSink.addAndGet(1))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfPipe1));
        Assertions.assertEquals(0, probeSink.get());
    }

    @Test
    public void testNonFatal_bitransformer_nonTriggered()
    {
        var probeSink = new AtomicInteger(0);
        var nfPipe0 = Flow.from(() -> 1);
        var nfPipe1 = Flow
            .from(() -> 1)
            .join(nfPipe0, nonFatal(Integer::sum))
            .sink(probeSink::addAndGet)
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfPipe1));
        Assertions.assertEquals(2, probeSink.get());
    }

    @Test
    public void testNonFatal_bitransformerWithConsumer()
    {
        var probeSink = new AtomicInteger(0);
        var probeConsumer = new AtomicInteger(0);
        var nfPipe0 = Flow.from(() -> 1);
        var nfPipe1 = Flow
            .from(() -> 1)
            .join(nfPipe0, nonFatal((a, b) -> {
                throw new Exception();
            }, e -> probeConsumer.addAndGet(1)))
            .sink(i -> probeSink.addAndGet(1))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfPipe1));
        Assertions.assertEquals(0, probeSink.get());
        Assertions.assertEquals(1, probeConsumer.get());
    }

    @Test
    public void testNonFatal_loader()
    {
        var nfSink = Flow.from(() -> 1)
            .sink(nonFatal((Loader<Integer>) i -> {
                throw new Exception();
            }))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfSink));
    }

    @Test
    public void testNonFatal_loader_nonTriggered()
    {
        var nfSink = Flow.from(() -> 1)
            .sink(nonFatal(i -> {}))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfSink));
    }

    @Test
    public void testNonFatal_loaderWithConsumer()
    {
        var probeConsumer = new AtomicInteger(0);
        var nfSink = Flow.from(() -> 1)
            .sink(nonFatal((Loader<Integer>) i -> {
                throw new Exception();
            }, e -> probeConsumer.addAndGet(1)))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(nfSink));
        Assertions.assertEquals(1, probeConsumer.get());
    }

    @Test
    public void testAdapter_pipe()
    {
        var flow = Flow.from(() -> List.of(1, 2, 3))
            .pipe(Flow.aggregate(i -> i + 1))
            .pipe(l -> l.stream().reduce(Integer::sum).orElseThrow())
            .collect()
        ;

        var output = Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(flow));
        Assertions.assertEquals(9, output.get(flow));
    }

    @Test
    public void testAdapter_sink()
    {
        var probeSink = new AtomicInteger(0);
        var flow = Flow.from(() -> List.of(1, 2, 3))
            .sink(Flow.aggregate((Loader<Integer>) probeSink::addAndGet))
        ;

        Assertions.assertDoesNotThrow(() -> Flow.runAsPipeline(flow));
        Assertions.assertEquals(6, probeSink.get());
    }
}
