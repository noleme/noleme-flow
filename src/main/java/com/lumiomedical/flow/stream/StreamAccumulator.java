package com.lumiomedical.flow.stream;

import com.lumiomedical.flow.FlowOut;
import com.lumiomedical.flow.Join;
import com.lumiomedical.flow.Pipe;
import com.lumiomedical.flow.Sink;
import com.lumiomedical.flow.actor.accumulator.Accumulator;
import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.node.SimpleNode;
import com.lumiomedical.flow.recipient.Recipient;

import java.util.function.Function;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public class StreamAccumulator <I, O> extends SimpleNode<Accumulator<I, O>> implements StreamIn<I>, FlowOut<O>
{
    /**
     * @param actor
     */
    public StreamAccumulator(Accumulator<I, O> actor)
    {
        super(actor);
    }

    @Override
    public <NO> Pipe<O, NO> into(Transformer<O, NO> transformer)
    {
        var pipe = new Pipe<>(transformer);
        this.bind(pipe);
        return pipe;
    }

    @Override
    public Sink<O> into(Loader<O> loader)
    {
        var sink = new Sink<>(loader);
        this.bind(sink);
        return sink;
    }

    @Override
    public <JI, JO> Join<O, JI, JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer)
    {
        return new Join<>(this, input, transformer);
    }

    @Override
    public <N> StreamGenerator<O, N> stream(Function<O, Generator<N>> generatorSupplier)
    {
        var pipe = new StreamGenerator<>(generatorSupplier);
        this.bind(pipe);
        return pipe;
    }

    /**
     *
     * @param loader
     * @return
     */
    public StreamAccumulator<I, O> drift(Loader<O> loader)
    {
        this.into(loader);
        return this;
    }

    @Override
    public Recipient<O> collect(String name)
    {
        Recipient<O> recipient = new Recipient<>(name);
        this.bind(recipient);
        return recipient;
    }

    /**
     *
     * @param name
     * @return
     */
    public StreamAccumulator<I, O> sample(String name)
    {
        this.collect(name);
        return this;
    }
}
