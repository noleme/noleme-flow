package com.lumiomedical.flow;

import com.lumiomedical.flow.etl.loader.Loader;
import com.lumiomedical.flow.node.BiNode;
import com.lumiomedical.flow.etl.transformer.BiTransformer;
import com.lumiomedical.flow.etl.transformer.Transformer;
import com.lumiomedical.flow.recipient.Recipient;

/**
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
        return Flow.join(this, input, transformer);
    }

    /**
     *
     * @param loader
     * @return
     */
    public Join<I1, I2, O> drift(Loader<O> loader)
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
    public Join<I1, I2, O> sample(String name)
    {
        this.collect(name);
        return this;
    }
}
