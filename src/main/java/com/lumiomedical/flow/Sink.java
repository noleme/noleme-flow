package com.lumiomedical.flow;

import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.node.SimpleNode;

/**
 * Sinks represent a final endpoint node in a DAG.
 * They accept an input from upstream but have no downstream.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public class Sink<I> extends SimpleNode<Loader<I>> implements FlowIn<I>
{
    /**
     *
     * @param actor
     */
    public Sink(Loader<I> actor)
    {
        super(actor);
    }
}
