package com.lumiomedical.flow;

import com.lumiomedical.flow.etl.loader.Loader;
import com.lumiomedical.flow.node.SimpleNode;

/**
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
