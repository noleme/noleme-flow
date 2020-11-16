package com.lumiomedical.flow;

import com.lumiomedical.flow.etl.loader.Loader;
import com.lumiomedical.flow.etl.transformer.BiTransformer;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.etl.transformer.Transformer;
import com.lumiomedical.flow.recipient.Recipient;

import java.util.UUID;

/**
 * Concept representing a Node with a potential downstream.
 * It features an "output" of type O which can be processed by a Pipe, Join or Sink.
 *
 * FlowOut nodes include Sources, Pipes and Joins.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public interface FlowOut<O> extends Node
{
    /**
     *
     * @param transformer
     * @param <NO>
     * @return
     */
    <NO> Pipe<O, NO> into(Transformer<O, NO> transformer);

    /**
     *
     * @param loader
     * @return
     */
    Sink<O> into(Loader<O> loader);

    /**
     *
     * @param input Flow with which to join the current flow.
     * @param transformer A bi-transformer function for performing the join.
     * @param <JI> Input type from another flow
     * @param <JO> Output type of the joined flow
     * @return
     */
    <JI, JO> Join<O, JI, JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer);

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
