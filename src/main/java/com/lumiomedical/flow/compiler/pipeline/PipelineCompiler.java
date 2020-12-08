package com.lumiomedical.flow.compiler.pipeline;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.FlowCompiler;
import com.lumiomedical.flow.compiler.pipeline.stream.StreamPipelineNode;
import com.lumiomedical.flow.logger.Logging;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.stream.StreamGenerator;
import com.lumiomedical.flow.stream.StreamNode;

import java.util.*;
import java.util.stream.Collectors;

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
        compiledNodes = this.compileStreams(compiledNodes);

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

    /**
     *
     * @param nodes
     * @return
     * @throws CompilationException
     */
    private List<Node> compileStreams(List<Node> nodes) throws CompilationException
    {
        //walk sorted nodes up -> bottom, for each node
        //  add each root stream node (generator) to a dictionary with an empty list
        //walk sorted nodes bottom -> up, for each node
        //  if node is stream node
        //    crawl upstream until root stream node (generator) is reached
        //    if root stream node (generator) does not exist
        //      error
        //    add node to root node list
        //    if node has non-stream parent (eg. as a stream join) and pivot node is null
        //      reference said parent as pivot node (the stream pipeline has to be inserted after that node)

        //walk root stream nodes, for each node
        //  initialize a StreamPipelineNode with:
        //  - the root stream node
        //  - the list of stream nodes
        //  - the pivot (if available)

        /* Look for every stream node generator and initialize a StreamPipelineNode for each one */
        Map<String, StreamPipelineNode> streamDictionary = nodes.stream()
            .filter(n -> n instanceof StreamGenerator)
            .collect(Collectors.toMap(Node::getUid, n -> new StreamPipelineNode((StreamGenerator)n)))
        ;

        /* If we couldn't identify a generator, then we don't need to compile any stream */
        if (streamDictionary.isEmpty())
            return nodes;

        Map<String, List<StreamPipelineNode>> pivotDictionary = new HashMap<>();

        /* Walk the compiled node list in reverse and build StreamPipelineNode contents */
        var reverseIterator = nodes.listIterator(nodes.size());
        while (reverseIterator.hasPrevious())
        {
            Node node = reverseIterator.previous();

            if (node instanceof StreamGenerator)
            {
                var pipelineNode = streamDictionary.get(node.getUid());

                computePotentialPivots(node, pipelineNode, pivotDictionary);
                reverseIterator.remove();
            }
            /* If the node is a stream node, we attempt to register it to a StreamPipelineNode and look for pivot non-stream nodes */
            else if (node instanceof StreamNode)
            {
                StreamGenerator generator = searchGenerator(node);

                if (generator == null)
                    throw new CompilationException("No parent stream generator could be found for stream node "+node.getUid());

                var pipelineNode = streamDictionary.get(generator.getUid());

                pipelineNode.push(node);
                computePotentialPivots(node, pipelineNode, pivotDictionary);
                reverseIterator.remove();
            }
            /* If we previously found the current node to be a pivot, we insert the corresponding StreamPipelineNode right after it */
            else if (pivotDictionary.containsKey(node.getUid()))
            {
                for (StreamPipelineNode streamNode : pivotDictionary.get(node.getUid()))
                {
                    if (streamNode.getPivot() != null)
                        continue;

                    reverseIterator.set(streamNode);
                    reverseIterator.add(node);
                    //reverseIterator.add(streamNode);
                    //reverseIterator.previous();
                    streamNode.setPivot(node.getUid());
                }
            }
        }

        return nodes;
    }

    /**
     *
     * @param node
     * @param pipelineNode
     * @param pivotDictionary
     */
    private static void computePotentialPivots(Node node, StreamPipelineNode pipelineNode, Map<String, List<StreamPipelineNode>> pivotDictionary)
    {
        var potentialPivots = getPivots(node);

        if (!potentialPivots.isEmpty())
        {
            if (pipelineNode.getPotentialPivots() == null)
                pipelineNode.setPotentialPivots(potentialPivots);
            else
                pipelineNode.getPotentialPivots().addAll(potentialPivots);

            for (String pivotUid : potentialPivots)
            {
                if (!pivotDictionary.containsKey(pivotUid))
                    pivotDictionary.put(pivotUid, new ArrayList<>());

                pivotDictionary.get(pivotUid).add(pipelineNode);
            }
        }
    }

    /**
     *
     * @param node
     * @return
     */
    private static StreamGenerator searchGenerator(Node node)
    {
        for (Node usn : node.getUpstream())
        {
            if (usn instanceof StreamGenerator)
                return (StreamGenerator) usn;
            else if (usn instanceof StreamNode)
                return searchGenerator(usn);
        }

        return null;
    }

    /**
     *
     * @param node
     * @return
     */
    private static Set<String> getPivots(Node node)
    {
        return node.getUpstream().stream()
            .filter(n -> !(n instanceof StreamNode))
            .map(Node::getUid)
            .collect(Collectors.toSet())
        ;
    }
}
