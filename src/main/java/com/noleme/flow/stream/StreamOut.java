package com.noleme.flow.stream;

import com.noleme.flow.FlowOut;
import com.noleme.flow.LeadOut;
import com.noleme.flow.actor.accumulator.Accumulator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;

import java.util.Collection;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/01
 */
public interface StreamOut<O> extends FlowOut<O>
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
     * Synonymous with into(Transformer), has the advantage of not allowing ambiguous lambdas.
     * @see #into(Transformer)
     */
    default <NO> StreamPipe<O, NO> pipe(Transformer<O, NO> transformer)
    {
        return this.into(transformer);
    }

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
    <JI, JO> StreamJoin<O, JI, JO> join(LeadOut<JI> input, BiTransformer<O, JI, JO> transformer);

    /**
     *
     * @param accumulator
     * @param <N>
     * @return
     */
    <N> StreamAccumulator<O, N> accumulate(Accumulator<O, N> accumulator);

    /**
     *
     * @return
     */
    default StreamAccumulator<O, Collection<O>> accumulate()
    {
        return this.accumulate(input -> input);
    }
}
