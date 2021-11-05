package com.noleme.flow;

import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.annotation.Experimental;
import com.noleme.flow.node.Node;
import com.noleme.flow.slice.SinkSlice;
import com.noleme.flow.slice.PipeSlice;
import com.noleme.flow.stream.StreamOut;

/**
 * Concept representing a {@link Node} with a potential downstream.
 * FlowOut nodes include {@link LeadOut} and {@link com.noleme.flow.stream.StreamOut} subtypes.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public interface FlowOut<O> extends Node
{
    /**
     * Binds the current node into a {@link Transformer}, resulting in a new {@link FlowOut} node.
     *
     * @param transformer a Transformer actor
     * @param <NO> Output type of the pipe node
     * @return the resulting Pipe node
     */
    <NO> FlowOut<NO> into(Transformer<O, NO> transformer);

    /**
     * Binds the current node into a {@link Loader}, resulting in a new {@link FlowOut} node.
     *
     * @param loader a Loader actor
     * @return the resulting Sink node
     */
    FlowIn<O> into(Loader<O> loader);

    /**
     * Synonymous with into(Transformer), has the advantage of not allowing ambiguous lambdas.
     * @see #into(Transformer)
     */
    default <NO> FlowOut<NO> pipe(Transformer<O, NO> transformer)
    {
        return this.into(transformer);
    }

    /**
     * Returns a nondescript {@link FlowOut} out of the provided {@link PipeSlice}.
     *
     * @param slice
     * @param <NO>
     * @return
     */
    @Experimental
    default <NO> FlowOut<NO> pipe(PipeSlice<O, NO> slice)
    {
        return slice.out(this);
    }

    @Experimental
    default StreamOut<O> asStream()
    {
        if (!(this instanceof StreamOut))
            throw new RuntimeException("Current non-specific flow is not of StreamOut type, observed type is "+this.getClass().getName());

        return (StreamOut<O>) this;
    }

    @Experimental
    default LeadOut<O> asLead()
    {
        if (!(this instanceof LeadOut))
            throw new RuntimeException("Current non-specific flow is not of LeadOut type, observed type is "+this.getClass().getName());

        return (LeadOut<O>) this;
    }

    /**
     * Synonymous with into(Loader), has the advantage of not allowing ambiguous lambdas.
     * @see #into(Loader)
     */
    default FlowIn<O> sink(Loader<O> loader)
    {
        return this.into(loader);
    }

    /**
     * Binds to the provided {@link SinkSlice}.
     *
     * @param slice
     * @return
     */
    @Experimental
    default FlowIn<?> sink(SinkSlice<O> slice)
    {
        return slice.out(this);
    }

    /**
     * Joins the current node with another flow using a bi-transformer join function.
     *
     * @param input Flow with which to join the current flow.
     * @param transformer A bi-transformer function for performing the join.
     * @param <JI> Input type from another flow
     * @param <JO> Output type of the joined flow
     * @return the resulting Join node
     */
    <JI, JO> FlowOut<JO> join(LeadOut<JI> input, BiTransformer<O, JI, JO> transformer);
}
