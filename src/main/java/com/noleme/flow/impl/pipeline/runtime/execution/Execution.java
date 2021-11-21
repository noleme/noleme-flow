package com.noleme.flow.impl.pipeline.runtime.execution;

import com.noleme.flow.Join;
import com.noleme.flow.Pipe;
import com.noleme.flow.Sink;
import com.noleme.flow.Source;
import com.noleme.flow.actor.Silent;
import com.noleme.flow.actor.accumulator.Accumulator;
import com.noleme.flow.actor.extractor.ExtractionException;
import com.noleme.flow.actor.extractor.Extractor;
import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.impl.pipeline.PipelineRunException;
import com.noleme.flow.impl.pipeline.runtime.heap.Heap;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingKey;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingNode;
import com.noleme.flow.interruption.InterruptionException;
import com.noleme.flow.io.input.InputExtractor;
import com.noleme.flow.io.input.Key;
import com.noleme.flow.io.output.Recipient;
import com.noleme.flow.node.BiNode;
import com.noleme.flow.node.Node;
import com.noleme.flow.node.SimpleNode;
import com.noleme.flow.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.noleme.flow.impl.pipeline.runtime.node.WorkingNode.upstreamOf;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
@SuppressWarnings("rawtypes")
public class Execution
{
    private static final Logger logger = LoggerFactory.getLogger(Execution.class);
    
    /**
     * Actually executes the Node passed as parameter.
     * The method is responsible for :
     * - extracting the node's input parameters from the Heap
     * - identifying the node's method signature
     * - upon success: push the return values to the Heap
     * - upon failure: either abort the Node, stop or abort the Runtime
     *
     * Returning false is designed as a mean for a "soft exit" which results in blocking downstream paths while continuing on running paths that are still valid.
     *
     * @param workingNode Target node
     * @param heap Heap object used for retrieving module parameters
     * @return true upon a successful execution, false otherwise
     * @throws PipelineRunException
     */
    @SuppressWarnings("unchecked")
    public boolean launch(WorkingNode<?> workingNode, Heap heap) throws PipelineRunException
    {
        Node node = workingNode.getNode();

        try {
            /*
             * The reason why we don't just implement a "launch" or "run" method in each Node subtype is so that we can have runtime-agnostic node definitions.
             * TODO: Maybe we can still preserve that prerequisite and have subtypes implement their run routine ; the tricky part is finding an agnostic way (eg. no knowledge of Heap or other runtime-specific construct) of doing input provisioning.
             */
            if (node instanceof Source)
                return this.launchSource((WorkingNode<Source<?>>) workingNode, heap);
            else if (node instanceof Pipe || node instanceof StreamPipe)
                return this.launchPipe((WorkingNode<SimpleNode<Transformer<?, ?>>>) workingNode, heap);
            else if (node instanceof Join || node instanceof StreamJoin)
                return this.launchJoin((WorkingNode<BiNode<BiTransformer<?, ?, ?>>>) workingNode, heap);
            else if (node instanceof Sink || node instanceof StreamSink)
                return this.launchSink((WorkingNode<SimpleNode<Loader<?>>>) workingNode, heap);
            else if (node instanceof StreamGenerator)
                return this.launchStreamGenerator((WorkingNode<StreamGenerator>) workingNode, heap);
            else if (node instanceof StreamAccumulator)
                return this.launchStreamAccumulator((WorkingNode<StreamAccumulator<?, ?>>) workingNode, heap);

            logger.error("Flow node #{} is of an unknown {} type", node.getUid(), node.getClass().getName());

            /*
             * Returning false is a "silent" failure mode, which can be used to signify a no-go for downstream node without stopping the rest of the graph execution.
             * Here we really want to crash the whole party since we apparently have an unknown node subtype.
             */
            throw new PipelineRunException("Unknown node type " + node.getClass().getName(), heap);
        }
        catch (InterruptionException e) {
            logger.debug("Flow node {}#{} has requested an interruption, blocking downstream nodes.", getName(node), node.getUid());

            return false;
        }
        catch (Exception e) {
            logger.error("Flow node {}#{} has thrown an error: {}", getName(node), node.getUid(), e.getMessage());

            throw new PipelineRunException("Node " + node.getClass().getName() + " " + getName(node) + "#" + node.getUid() + " has thrown an exception. (" + e.getClass() + ")", e, heap);
        }
    }

    /**
     *
     * @param source
     * @param heap
     * @return
     * @throws Exception
     */
    private boolean launchSource(WorkingNode<Source<?>> source, Heap heap) throws Exception
    {
        Extractor extractor = source.getNode().getActor();

        logger.debug("Launching flow source {}#{} of extractor {}", getName(source), source.getUid(), extractor.getClass().getName());

        /* If the extractor is an InputExtractor, the output value comes from the provided input instead of the extractor itself ; the extractor only holds a reference to the expected input */
        if (extractor instanceof InputExtractor)
        {
            Key<?> key = ((InputExtractor<?>) extractor).getKey();
            if (!heap.hasInput(key))
                throw new ExtractionException("The InputExtractor in node #" + source.getUid() + " couldn't find its expected input");

            heap.push(source.getKey(), heap.getInput(key), source.getDownstream().size());
        }
        /* Otherwise normal rules apply */
        else
            heap.push(source.getKey(), extractor.extract(), source.getDownstream().size());

        return true;
    }

    /**
     *
     * @param pipe
     * @param heap
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private boolean launchPipe(WorkingNode<SimpleNode<Transformer<?, ?>>> pipe, Heap heap) throws Exception
    {
        Transformer transformer = pipe.getNode().getActor();

        if (transformer.getClass().getAnnotation(Silent.class) == null)
            logger.debug("Launching flow pipe {}#{} of transformer {}", getName(pipe), pipe.getUid(), transformer.getClass().getName());

        Object input = heap.consume(heapKeyOf(pipe));
        heap.push(pipe.getKey(), transformer.transform(input), pipe.getDownstream().size());
        return true;
    }

    /**
     *
     * @param join
     * @param heap
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private boolean launchJoin(WorkingNode<BiNode<BiTransformer<?, ?, ?>>> join, Heap heap) throws Exception
    {
        BiTransformer transformer = join.getNode().getActor();

        WorkingNode<?> upstream1 = upstreamOf(join, 0);
        WorkingNode<?> upstream2 = upstreamOf(join, 1);

        logger.debug(
            "Launching flow join {}#{} of upstream flows {}#{} and {}#{}",
            getName(join),
            join.getUid(),
            getName(upstream1),
            upstream1.getUid(),
            getName(upstream2),
            upstream2.getUid()
        );

        Object input1 = heap.consume(upstream1.getKey());
        Object input2 = heap.peek(upstream2.getKey()); //FIXME: this currently results in nodes not being GCed in a stream context (counter is sized as if streams counted for 1, so we peek, so we neve reach 0)
        heap.push(join.getKey(), transformer.transform(input1, input2), join.getDownstream().size());
        return true;
    }

    /**
     *
     * @param sink
     * @param heap
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private boolean launchSink(WorkingNode<SimpleNode<Loader<?>>> sink, Heap heap) throws Exception
    {
        Loader loader = sink.getNode().getActor();

        if (loader.getClass().getAnnotation(Silent.class) == null)
            logger.debug("Launching flow sink {}#{} of loader {}", getName(sink), sink.getUid(), loader.getClass().getName());

        Object input = heap.consume(heapKeyOf(sink));

        /* If the sink is a Recipient, the output value comes from the provided input instead of the extractor itself ; the extractor only holds a reference to the expected input */
        if (sink.getNode() instanceof Recipient)
        {
            var identifier = ((Recipient) sink.getNode()).getIdentifier();
            heap.setOutput(identifier, input);
        }
        else
            loader.load(input);

        return true;
    }

    /**
     *
     * @param generatorNode
     * @param heap
     * @return
     * @throws Exception
     */
    private boolean launchStreamGenerator(WorkingNode<StreamGenerator> generatorNode, Heap heap) throws Exception
    {
        Generator generator = heap.getStreamGenerator(generatorNode);

        logger.debug("Launching flow stream generator {}#{} at offset {} with generator {}", getName(generatorNode), generatorNode.getUid(), generatorNode.getKey().offset(), generator.getClass().getName());

        heap.push(generatorNode.getKey(), generator.generate(), generatorNode.getDownstream().size());
        return true;
    }

    /**
     *
     * @param node
     * @param heap
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private boolean launchStreamAccumulator(WorkingNode<StreamAccumulator<?, ?>> node, Heap heap) throws Exception
    {
        Accumulator accumulator = node.getNode().getActor();

        logger.debug("Launching flow stream accumulator {}#{} of accumulator {}", getName(node), node.getUid(), node.getClass().getName());

        Collection<Object> input = heap.consumeAll(upstreamOf(node).getKey().withoutOffset());

        WorkingKey accumulationKey = WorkingKey.of(node, node.getKey().offset(), node.getKey().parent() != null ? node.getKey().parent().parent() : null);

        heap.push(accumulationKey, accumulator.accumulate(input), node.getDownstream().size());
        return true;
    }

    /**
     *
     * @param node
     * @return
     */
    private static String getName(Node node)
    {
        return node.getName() != null ? node.getName() : "";
    }

    /**
     *
     * @param node
     * @return
     */
    private static WorkingKey heapKeyOf(WorkingNode<?> node)
    {
        return heapKeyOf(node, 0);
    }

    /**
     *
     * @param node
     * @param index
     * @return
     */
    private static WorkingKey heapKeyOf(WorkingNode<?> node, int index)
    {
        /*
        var upstreamNode = upstreamOf(node, index);
        return upstreamNode.getNode() instanceof StreamOut
            ? upstreamNode.getKey().atOffset(node.getKey().offset(), node.getKey().parent())
            : upstreamNode.getKey()
        ;
         */
        return upstreamOf(node, index).getKey();
    }
}
