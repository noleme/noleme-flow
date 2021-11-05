package com.noleme.flow;

import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.node.SimpleNode;

/**
 * Sinks represent a final endpoint node in a DAG.
 * They accept an input from upstream but have no downstream.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public class Sink<I> extends SimpleNode<Loader<I>> implements CurrentIn<I>
{
    /**
     *
     * @param actor
     */
    public Sink(Loader<I> actor)
    {
        super(actor);
    }

    /**
     *
     * @param name
     * @return
     */
    public Sink<I> name(String name)
    {
        this.name = name;
        return this;
    }
}
