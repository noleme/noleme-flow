package com.lumiomedical.flow.stream;

import com.lumiomedical.flow.FlowOut;
import com.lumiomedical.flow.actor.accumulator.Accumulator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.node.BiNode;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/01
 */
public class StreamJoin<I1, I2, O> extends BiNode implements StreamOut<O>, StreamNode
{
    private final BiTransformer<I1, I2, O> actor;

    /**
     *
     * @param input1
     * @param input2
     * @param actor
     */
    public StreamJoin(StreamOut<I1> input1, FlowOut<I2> input2, BiTransformer<I1, I2, O> actor)
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
    public StreamJoin<I1, I2, O> drift(Loader<O> loader)
    {
        this.into(loader);
        return this;
    }
}
