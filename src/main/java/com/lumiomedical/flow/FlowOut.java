package com.lumiomedical.flow;

import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.recipient.Recipient;
import com.lumiomedical.flow.stream.StreamGenerator;

import java.util.UUID;
import java.util.function.Function;

/**
 * Concept representing a Node with a potential downstream.
 * It features an "output" of type O which can be processed by a Pipe, Join or Sink.
 *
 * FlowOut nodes include Sources, Pipes and Joins.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public interface FlowOut <O> extends Node
{
    /**
     * Binds the current node into a Transformer, resulting in a new Pipe node.
     *
     * @param transformer a Transformer actor
     * @param <NO> Output type of the pipe node
     * @return the resulting Pipe node
     */
    <NO> Pipe<O, NO> into(Transformer<O, NO> transformer);

    /**
     * Binds the current node into a Loader, resulting in a new Sink node.
     *
     * @param loader a Loader actor
     * @return the resulting Sink node
     */
    Sink<O> into(Loader<O> loader);

    /**
     * Synonymous with into(Loader), has the advantage of not allowing ambiguous lambdas.
     * @see #into(Loader)
     */
    default Sink<O> sink(Loader<O> loader)
    {
        return this.into(loader);
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
    <JI, JO> Join<O, JI, JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer);

    /**
     * Initiates a stream from the current node, results in a new StreamGenerator node.
     *
     * @param generatorSupplier a Generator creation function
     * @param <NO> Output type of the stream generator node
     * @return the resulting StreamGenerator node
     */
    <NO> StreamGenerator<O, NO> stream(Function<O, Generator<NO>> generatorSupplier);

    /**
     *
     * @param name
     * @return
     */
    Recipient<O> collect(String name);

    /**
     *
     * @return
     */
    default Recipient<O> collect()
    {
        return this.collect(UUID.randomUUID().toString());
    }
}
