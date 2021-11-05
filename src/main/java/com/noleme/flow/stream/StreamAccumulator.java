package com.noleme.flow.stream;

import com.noleme.flow.*;
import com.noleme.flow.actor.accumulator.Accumulator;
import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.io.output.Recipient;
import com.noleme.flow.node.SimpleNode;

import java.util.function.Function;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public class StreamAccumulator<I, O> extends SimpleNode<Accumulator<I, O>> implements StreamIn<I>, LeadOut<O>
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
    public <JI, JO> Join<O, JI, JO> join(LeadOut<JI> input, BiTransformer<O, JI, JO> transformer)
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
    public StreamAccumulator<I, O> driftSink(Loader<O> loader)
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

    /**
     *
     * @param name
     * @return
     */
    public StreamAccumulator<I, O> name(String name)
    {
        this.name = name;
        return this;
    }
}
