package com.lumiomedical.flow;

import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.interruption.Interruption;
import com.lumiomedical.flow.node.BiNode;
import com.lumiomedical.flow.recipient.Recipient;
import com.lumiomedical.flow.stream.StreamGenerator;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Joins are a point of passage joining two upstream branchs of a DAG.
 * They accept two inputs from upstream, produce a joined output using a provided BiTransformer implementation, then pass it downstream.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public class Join<I1, I2, O> extends BiNode implements FlowOut<O>
{
    private final BiTransformer<I1, I2, O> actor;

    /**
     *
     * @param input1
     * @param input2
     * @param actor
     */
    public Join(FlowOut<I1> input1, FlowOut<I2> input2, BiTransformer<I1, I2, O> actor)
    {
        super(input1, input2);
        this.actor = actor;
    }

    /**
     *
     * @return
     */
    public BiTransformer<I1, I2, O> getActor()
    {
        return this.actor;
    }

    @Override
    public <N> Pipe<O, N> into(Transformer<O, N> transformer)
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
    public Join<I1, I2, O> drift(Loader<O> loader)
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
    public Join<I1, I2, O> sample(String name)
    {
        this.collect(name);
        return this;
    }


    /**
     *
     * @return
     */
    public Pipe<O, O> interrupt()
    {
        return this.into(new Interruption<>());
    }

    /**
     *
     * @param predicate
     * @return
     */
    public Pipe<O, O> interruptIf(Predicate<O> predicate)
    {
        return this.into(new Interruption<>(predicate));
    }
}
