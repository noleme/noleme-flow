package com.lumiomedical.flow.compiler.pipeline;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.FlowCompiler;
import com.lumiomedical.flow.logger.Logging;
import com.lumiomedical.flow.node.Node;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class PipelineCompiler implements FlowCompiler<PipelineRuntime>
{
    @Override
    public PipelineRuntime compile(Collection<Node> inputNodes) throws CompilationException
    {
        return new PipelineRuntime(this.compileNodes(inputNodes));
    }

    /**
     *
     * @param inputNodes
     * @return
     * @throws CompilationException
     */
    public List<Node> compileNodes(Collection<Node> inputNodes) throws CompilationException
    {
        Set<Node> startingPoints = this.collectStartingPoints(inputNodes);
        List<Node> compiledNodes = this.sortGraph(startingPoints);

        return compiledNodes;
    }

    /**
     * An implementation of a topological sort in order to assert that the graph is a DAG.
     *
     * @param startingPoints Starting points from which to attempt the traversal
     * @return The whole graph in the form of a sorted list of executable nodes
     * @throws CompilationException
     */
    private List<Node> sortGraph(Collection<Node> startingPoints) throws CompilationException
    {
        LinkedList<Node> sorted = new LinkedList<>();
        Set<String> temporary = new HashSet<>();
        Set<String> permanent = new HashSet<>();

        for (Node n : startingPoints)
        {
            if (!temporary.contains(n.getUid()) && !permanent.contains(n.getUid()))
                this.checkEdgeIntegrity(n, temporary, permanent, sorted);
        }

        Logging.logger.debug("Flow execution graph contains "+sorted.size()+" nodes");

        return sorted;
    }

    /**
     *
     * @param n
     * @param temp
     * @param perm
     * @param sorted
     * @throws CompilationException
     */
    private void checkEdgeIntegrity(Node n, Set<String> temp, Set<String> perm, LinkedList<Node> sorted) throws CompilationException
    {
        if (temp.contains(n.getUid()))
            throw new CompilationException("A circular reference has been detected, the graph is not suitable for execution.");

        if (!perm.contains(n.getUid()))
        {
            temp.add(n.getUid());
            for (Node dsn : n.getRequiredBy())
                this.checkEdgeIntegrity(dsn, temp, perm, sorted);
            perm.add(n.getUid());
            temp.remove(n.getUid());
            sorted.addFirst(n);
        }
    }

    /**
     *
     * @param inputNodes
     * @return
     */
    private Set<Node> collectStartingPoints(Collection<Node> inputNodes)
    {
        Set<Node> startingPoints = new HashSet<>();
        Queue<Node> compileQueue = new LinkedList<>(inputNodes);
        Set<String> compiledNodes = new HashSet<>();

        while (!compileQueue.isEmpty())
        {
            Node current = compileQueue.poll();
            compiledNodes.add(current.getUid());

            /* For each node we check if it's a starting point, if it isn't, we push its requirements to the compilation queue */
            if (current.getRequirements().isEmpty())
                startingPoints.add(current);
            else
                compileQueue.addAll(current.getRequirements());

            /*
             * We also explore downstream nodes in order to pick up any left-out branch.
             * Typically, if the compilation is given the starting point directly, we still need to check if there aren't other starting points that were bound further down the tree.
             */
            for (Node downstream : current.getDownstream())
            {
                /* We keep track of which downstream node was already explored */
                if (!compiledNodes.contains(downstream.getUid()))
                    compileQueue.add(downstream);
            }
        }

        Logging.logger.debug("Collected "+startingPoints.size()+" flow entry nodes");

        return startingPoints;
    }

}
