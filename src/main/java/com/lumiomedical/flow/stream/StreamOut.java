package com.lumiomedical.flow.stream;

import com.lumiomedical.flow.FlowOut;
import com.lumiomedical.flow.actor.accumulator.Accumulator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.node.Node;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/01
 */
public interface StreamOut<O> extends Node
{
    /**
     * Binds the current node into a Transformer, resulting in a new StreamPipe node.
     *
     * @param transformer a Transformer actor
     * @param <NO> Output type of the pipe node
     * @return the resulting StreamPipe node
     */
    <NO> StreamPipe<O, NO> into(Transformer<O, NO> transformer);

    /**
     * Binds the current node into a Loader, resulting in a new StreamSink node.
     *
     * @param loader a Loader actor
     * @return the resulting StreamSink node
     */
    StreamSink<O> into(Loader<O> loader);

    /**
     * Synonymous with into(Loader), has the advantage of not allowing ambiguous lambdas.
     * @see #into(Loader)
     */
    default StreamSink<O> sink(Loader<O> loader)
    {
        return this.into(loader);
    }

    /**
     * Joins the current stream node with another non-stream flow using a bi-transformer join function.
     *
     * @param input Flow with which to join the current flow.
     * @param transformer A bi-transformer function for performing the join.
     * @param <JI> Input type from another flow
     * @param <JO> Output type of the joined flow
     * @return
     */
    <JI, JO> StreamJoin<O, JI, JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer);

    /**
     *
     * @param accumulator
     * @param <N>
     * @return
     */
    <N> StreamAccumulator<O, N> accumulate(Accumulator<O, N> accumulator);
}
