package com.lumiomedical.flow.impl.pipeline.compiler.pass;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.impl.pipeline.compiler.stream.StreamPipeline;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.stream.StreamGenerator;
import com.lumiomedical.flow.stream.StreamNode;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/13
 */
public class StreamAggregationPass implements PipelineCompilerPass
{
    @Override
    public Collection<Node> run(Collection<Node> nodes) throws CompilationException
    {
        LinkedList<Node> nodeList = nodes instanceof LinkedList
            ? (LinkedList<Node>)nodes
            : new LinkedList<>(nodes)
        ;

        return this.compileStreams(nodeList);
    }

    /**
     *
     * @param nodes
     * @return
     * @throws CompilationException
     */
    private LinkedList<Node> compileStreams(LinkedList<Node> nodes) throws CompilationException
    {
        /* Look for every stream node generator and initialize a StreamPipelineNode for each one */
        Map<String, StreamPipeline> streamDictionary = nodes.stream()
            .filter(n -> n instanceof StreamGenerator)
            .collect(Collectors.toMap(Node::getUid, n -> new StreamPipeline((StreamGenerator)n)))
        ;

        /* If we couldn't identify a generator, then we don't need to compile any stream */
        if (streamDictionary.isEmpty())
            return nodes;

        Map<String, List<StreamPipeline>> pivotDictionary = new HashMap<>();

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
                for (StreamPipeline streamNode : pivotDictionary.get(node.getUid()))
                {
                    if (streamNode.getPivot() != null)
                        continue;

                    reverseIterator.set(streamNode);
                    reverseIterator.add(node);
                    streamNode.setPivot(node.getUid());
                }
            }

            /* If we reached the end of the nodes stack, we have to make sure there aren't any remaining stream nodes without pivots (stream entry points) */
            if (!reverseIterator.hasPrevious())
            {
                streamDictionary.values().stream()
                    .filter(n -> n.getPivot() == null)
                    .forEach(nodes::push)
                ;
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
    private static void computePotentialPivots(Node node, StreamPipeline pipelineNode, Map<String, List<StreamPipeline>> pivotDictionary)
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
    public static StreamGenerator searchGenerator(Node node)
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
