package com.lumiomedical.flow;

import com.lumiomedical.flow.etl.loader.Loader;
import com.lumiomedical.flow.etl.transformer.BiTransformer;
import com.lumiomedical.flow.node.SimpleNode;
import com.lumiomedical.flow.etl.transformer.Transformer;
import com.lumiomedical.flow.recipient.Recipient;

/**
 * Pipes are a point of passage in a DAG.
 * They accept an input from upstream which they can alter and pass downstream.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/28
 */
public class Pipe<I, O> extends SimpleNode<Transformer<I, O>> implements FlowIn<I>, FlowOut<O>
{
    /**
     *
     * @param actor
     */
    public Pipe(Transformer<I, O> actor)
    {
        super(actor);
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
        return Flow.join(this, input, transformer);
    }

    /**
     *
     * @param loader
     * @return
     */
    public Pipe<I, O> drift(Loader<O> loader)
    {
        var sink = new Sink<>(loader);
        this.bind(sink);
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
    public Pipe<I, O> sample(String name)
    {
        this.collect(name);
        return this;
    }
}
