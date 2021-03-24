package com.noleme.flow.node;

import java.util.Collection;
import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public interface Node extends Comparable<Node>
{
    /**
     * Returns a unique identifier for the node within a flow DAG.
     *
     * @return the node UID
     */
    String getUid();

    /**
     * Returns the list of downstream nodes, ie. nodes which require the current node's output in order to be executed.
     *
     * @return the list of downstream nodes
     */
    List<Node> getDownstream();

    /**
     * Returns the list of upstream nodes, ie. nodes which output is required in order to execute the current node.
     * For Source nodes, this should be empty.
     * For Pipe and Sink nodes, this should be a list of size 1.
     * For Join nodes, this should be a list of size 2.
     *
     * @return the list of upstream nodes
     */
    List<Node> getUpstream();

    /**
     * Returns the list of downstream requirements over the current node, ie. nodes which cannot be executed until the current node is.
     * There can be any number of requirements for a given node.
     *
     * @return the list of downstream requirement nodes
     */
    List<Node> getRequiredBy();

    /**
     * Returns the list of upstream requirement nodes, ie. nodes which need to be executed before, but which outputs aren't required for execution.
     * There can be any number of requirements for a given node.
     *
     * @return the list of upstream requirement nodes
     */
    List<Node> getRequirements();

    /**
     * Requests for the current node to be executed after the provided node.
     * It should result in the current node having a requirement towards the provided node.
     * @see #getRequirements()
     *
     * @param other the node after which it has to be executed
     * @return the current node
     */
    Node after(Node other);

    /**
     * Requests for the current node to be executed after the provided nodes.
     * It should result in the current node having a requirement towards each node in the provided collection.
     * @see #getRequirements()
     *
     * @param others the nodes after which it has to be executed
     * @return the current node
     */
    Node after(Collection<Node> others);

    @Override
    default int compareTo(Node o)
    {
        return this.getUid().compareTo(o.getUid());
    }
}
