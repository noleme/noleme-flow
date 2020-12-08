package com.lumiomedical.flow.stream;

import com.lumiomedical.flow.FlowIn;
import com.lumiomedical.flow.FlowOut;
import com.lumiomedical.flow.actor.accumulator.Accumulator;
import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.node.SimpleNode;

import java.util.function.Function;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public class StreamGenerator <I, O> extends SimpleNode<Function<I, Generator<O>>> implements FlowIn<I>, StreamOut<O>
{
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
    public StreamGenerator<I, O> drift(Loader<O> loader)
    {
        this.into(loader);
        return this;
    }
}
