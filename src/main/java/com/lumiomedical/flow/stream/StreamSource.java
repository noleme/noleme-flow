package com.lumiomedical.flow.stream;

import com.lumiomedical.flow.FlowOut;
import com.lumiomedical.flow.actor.accumulator.Accumulator;
import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.interruption.Interruption;
import com.lumiomedical.flow.node.SimpleNode;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/01
 */
public class StreamSource<O> extends SimpleNode<Supplier<Generator<O>>> implements StreamOut<O>, StreamNode
{
    /**
     * @param generatorSupplier
     */
    public StreamSource(Supplier<Generator<O>> generatorSupplier)
    {
        super(generatorSupplier);
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
    public StreamSource<O> drift(Loader<O> loader)
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
}
