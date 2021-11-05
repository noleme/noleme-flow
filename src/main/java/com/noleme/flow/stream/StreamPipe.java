package com.noleme.flow.stream;

import com.noleme.flow.LeadOut;
import com.noleme.flow.actor.accumulator.Accumulator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.interruption.Interruption;
import com.noleme.flow.node.SimpleNode;

import java.util.function.Predicate;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/01
 */
public class StreamPipe<I, O> extends SimpleNode<Transformer<I, O>> implements StreamIn<I>, StreamOut<O>, StreamNode
{
    /**
     * @param actor
     */
    public StreamPipe(Transformer<I, O> actor)
    {
        super(actor);
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
    public <JI, JO> StreamJoin<O, JI, JO> join(LeadOut<JI> input, BiTransformer<O, JI, JO> transformer)
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
    public StreamPipe<I, O> driftSink(Loader<O> loader)
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

    /**
     *
     * @param name
     * @return
     */
    public StreamPipe<I, O> name(String name)
    {
        this.name = name;
        return this;
    }
}
