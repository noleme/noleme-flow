package com.lumiomedical.flow.node;

import java.util.Collection;
import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public interface Node extends Comparable<Node>
{
    String getUid();

    List<Node> getDownstream();

    List<Node> getUpstream();

    List<Node> getRequirements();

    List<Node> getRequiredBy();

    Node after(Node other);

    Node after(Collection<Node> others);

    @Override
    default int compareTo(Node o)
    {
        return this.getUid().compareTo(o.getUid());
    }
}
