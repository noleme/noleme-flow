package com.noleme.flow;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.interruption.Interruption;
import com.noleme.flow.io.output.Recipient;
import com.noleme.flow.node.BiNode;
import com.noleme.flow.stream.StreamGenerator;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Joins are a point of passage joining two upstream branchs of a DAG.
 * They accept two inputs from upstream, produce a joined output using a provided {@link BiTransformer} implementation, then pass it downstream.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public class Join<I1, I2, O> extends BiNode implements LeadOut<O>
{
    private final BiTransformer<I1, I2, O> actor;

    /**
     *
     * @param input1
     * @param input2
     * @param actor
     */
    public Join(FlowOut<I1> input1, LeadOut<I2> input2, BiTransformer<I1, I2, O> actor)
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
    public Join<I1, I2, O> driftSink(Loader<O> loader)
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

    /**
     *
     * @param name
     * @return
     */
    public Join<I1, I2, O> name(String name)
    {
        this.name = name;
        return this;
    }
}
