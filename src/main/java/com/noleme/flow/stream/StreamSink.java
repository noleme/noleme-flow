package com.noleme.flow.stream;

import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.node.SimpleNode;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/01
 */
public class StreamSink<I> extends SimpleNode<Loader<I>> implements StreamIn<I>, StreamNode
{
    /**
     * @param actor
     */
    public StreamSink(Loader<I> actor)
    {
        super(actor);
    }
}
