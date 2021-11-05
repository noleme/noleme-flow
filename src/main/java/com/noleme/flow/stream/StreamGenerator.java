package com.noleme.flow.stream;

import com.noleme.flow.FlowIn;
import com.noleme.flow.FlowOut;
import com.noleme.flow.actor.accumulator.Accumulator;
import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.interruption.Interruption;
import com.noleme.flow.node.SimpleNode;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public class StreamGenerator<I, O> extends SimpleNode<Function<I, Generator<O>>> implements FlowIn<I>, StreamOut<O>
{
    private int maxParallelism = 1;

    /**
     * @param generatorSupplier
     */
    public StreamGenerator(Function<I, Generator<O>> generatorSupplier)
    {
        super(generatorSupplier);
    }

    /**
     *
     * @param input
     * @return
     */
    public Generator<O> produceGenerator(I input)
    {
        return this.getActor().apply(input);
    }

    @Override
    public <NO> StreamPipe<O, NO> into(Transformer<O, NO> transformer)
    {
        var pipe = new StreamPipe<>(transformer);
        this.bind(pipe);
        return pipe;
    }

    @Override
    public StreamSink<O> into(Loader<O> loader)
    {
        var sink = new StreamSink<>(loader);
        this.bind(sink);
        return sink;
    }

    @Override
    public <JI, JO> StreamJoin<O, JI, JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer)
    {
        return new StreamJoin<>(this, input, transformer);
    }

    @Override
    public <N> StreamAccumulator<O, N> accumulate(Accumulator<O, N> accumulator)
    {
        var acc = new StreamAccumulator<>(accumulator);
        this.bind(acc);
        return acc;
    }

    /**
     *
     * @param loader
     * @return
     */
    public StreamGenerator<I, O> driftSink(Loader<O> loader)
    {
        this.into(loader);
        return this;
    }

    /**
     *
     * @return
     */
    public StreamPipe<O, O> interrupt()
    {
        return this.into(new Interruption<>());
    }

    /**
     *
     * @param predicate
     * @return
     */
    public StreamPipe<O, O> interruptIf(Predicate<O> predicate)
    {
        return this.into(new Interruption<>(predicate));
    }

    public int getMaxParallelism()
    {
        return this.maxParallelism;
    }

    /**
     *
     * @param factor
     * @return
     */
    public StreamGenerator<I, O> setMaxParallelism(int factor)
    {
        if (factor < 1)
            throw new RuntimeException("StreamGenerator maximum parallelism factor is expected to be larger or equal to 1");

        this.maxParallelism = factor;
        return this;
    }

    /**
     *
     * @param name
     * @return
     */
    public StreamGenerator<I, O> name(String name)
    {
        this.name = name;
        return this;
    }
}
