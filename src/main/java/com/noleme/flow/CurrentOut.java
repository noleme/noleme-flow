package com.noleme.flow;

import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.annotation.Experimental;
import com.noleme.flow.interruption.Interruption;
import com.noleme.flow.node.Node;
import com.noleme.flow.slice.SinkSlice;
import com.noleme.flow.slice.PipeSlice;
import com.noleme.flow.stream.StreamOut;

import java.util.function.Predicate;

/**
 * Concept representing a {@link Node} with a potential downstream.
 * CurrentOut nodes include {@link FlowOut} and {@link com.noleme.flow.stream.StreamOut} subtypes.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public interface CurrentOut<O> extends Node
{
    /**
     * Binds the current node into a {@link Transformer}, resulting in a new {@link CurrentOut} node.
     *
     * @param transformer a Transformer actor
     * @param <NO> Output type of the pipe node
     * @return the resulting Pipe node
     */
    <NO> CurrentOut<NO> into(Transformer<O, NO> transformer);

    /**
     * Binds the current node into a {@link Loader}, resulting in a new {@link CurrentOut} node.
     *
     * @param loader a Loader actor
     * @return the resulting Sink node
     */
    CurrentIn<O> into(Loader<O> loader);

    /**
     * Binds the current node into an {@link Interruption}.
     * It will trigger a local interruption and allow the rest of the flow to continue.
     *
     * @return
     */
    default CurrentOut<O> interrupt()
    {
        return this.into(new Interruption<>());
    }

    /**
     * Binds the current node into an {@link Interruption}.
     * It will trigger a local interruption if the provided predicate is satisfied, and allow the rest of the flow to continue.
     *
     * @param predicate
     * @return
     */
    default CurrentOut<O> interruptIf(Predicate<O> predicate)
    {
        return this.into(new Interruption<>());
    }

    /**
     * Synonymous with into(Transformer), has the advantage of not allowing ambiguous lambdas.
     * @see #into(Transformer)
     */
    default <NO> CurrentOut<NO> pipe(Transformer<O, NO> transformer)
    {
        return this.into(transformer);
    }

    /**
     * Returns a nondescript {@link CurrentOut} out of the provided {@link PipeSlice}.
     *
     * @param slice
     * @param <NO>
     * @return
     */
    @Experimental
    default <NO> CurrentOut<NO> pipe(PipeSlice<O, NO> slice)
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
    default FlowOut<O> asFlow()
    {
        if (!(this instanceof FlowOut))
            throw new RuntimeException("Current non-specific flow is not of FlowOut type, observed type is "+this.getClass().getName());

        return (FlowOut<O>) this;
    }

    /**
     * Synonymous with into(Loader), has the advantage of not allowing ambiguous lambdas.
     * @see #into(Loader)
     */
    default CurrentIn<O> sink(Loader<O> loader)
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
    default CurrentIn<?> sink(SinkSlice<O> slice)
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
    <JI, JO> CurrentOut<JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer);
}
