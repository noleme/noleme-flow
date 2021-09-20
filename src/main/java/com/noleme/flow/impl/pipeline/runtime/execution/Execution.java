package com.noleme.flow.impl.pipeline.runtime.execution;

import com.noleme.flow.Join;
import com.noleme.flow.Pipe;
import com.noleme.flow.Sink;
import com.noleme.flow.Source;
import com.noleme.flow.actor.accumulator.AccumulationException;
import com.noleme.flow.actor.accumulator.Accumulator;
import com.noleme.flow.actor.extractor.ExtractionException;
import com.noleme.flow.actor.extractor.Extractor;
import com.noleme.flow.actor.generator.GenerationException;
import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.loader.LoadingException;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.TransformationException;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.impl.pipeline.PipelineRunException;
import com.noleme.flow.impl.pipeline.runtime.heap.Heap;
import com.noleme.flow.impl.pipeline.runtime.node.OffsetNode;
import com.noleme.flow.interruption.InterruptionException;
import com.noleme.flow.io.input.InputExtractor;
import com.noleme.flow.io.output.Recipient;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

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
     * @param node Target node
     * @param heap Heap object used for retrieving module parameters
     * @return true upon a successful execution, false otherwise
     * @throws PipelineRunException
     */
    public boolean launch(Node node, Heap heap) throws PipelineRunException
    {
        try {
            /*
             * The reason why we don't just implement a "launch" or "run" method in each Node subtype is so that we can have runtime-agnostic node definitions.
             * TODO: Maybe we can still preserve that prerequisite and have subtypes implement their run routine ; the tricky part is finding an agnostic way (eg. no knowledge of Heap or other runtime-specific construct) of doing input provisioning.
             */
            if (node instanceof Source)
                return this.launchSource((Source<?>) node, heap);
            else if (node instanceof Pipe)
                return this.launchPipe((Pipe<?, ?>) node, heap);
            else if (node instanceof Join)
                return this.launchJoin((Join<?, ?, ?>) node, heap);
            else if (node instanceof Sink)
                return this.launchSink((Sink<?>) node, heap);
            else if (node instanceof OffsetNode)
                return this.launchOffset((OffsetNode) node, heap);
            else if (node instanceof StreamAccumulator)
                return this.launchStreamAccumulator((StreamAccumulator<?, ?>) node, heap);

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
        catch (ExtractionException | TransformationException | LoadingException | GenerationException | AccumulationException e) {
            logger.error("Flow node {}#{} has thrown an error: {}", getName(node), node.getUid(), e.getMessage());

            throw new PipelineRunException("Node " + node.getClass().getName() + " " + getName(node) + "#" + node.getUid() + " has thrown an exception. (" + e.getClass() + ")", e, heap);
        }
    }

    /**
     *
     * @param source
     * @param heap
     * @return
     * @throws ExtractionException
     */
    private boolean launchSource(Source<?> source, Heap heap) throws ExtractionException
    {
        Extractor extractor = source.getActor();

        logger.debug("Launching flow source {}#{} of extractor {}", getName(source), source.getUid(), extractor.getClass().getName());

        /* If the extractor is an InputExtractor, the output value comes from the provided input instead of the extractor itself ; the extractor only holds a reference to the expected input */
        if (extractor instanceof InputExtractor)
        {
            var identifier = ((InputExtractor<?>) extractor).getIdentifier();
            if (!heap.hasInput(identifier))
                throw new ExtractionException("The InputExtractor in node #"+source.getUid()+" couldn't find its expected input "+identifier);

            heap.push(source.getUid(), heap.getInput(identifier), source.getDownstream().size());
        }
        /* Otherwise normal rules apply */
        else
            heap.push(source.getUid(), extractor.extract(), source.getDownstream().size());

        return true;
    }

    /**
     *
     * @param pipe
     * @param heap
     * @return
     * @throws TransformationException
     */
    @SuppressWarnings("unchecked")
    private boolean launchPipe(Pipe<?, ?> pipe, Heap heap) throws TransformationException
    {
        Transformer transformer = pipe.getActor();

        logger.debug("Launching flow pipe {}#{} of transformer {}", getName(pipe), pipe.getUid(), transformer.getClass().getName());

        Object input = heap.consume(pipe.getSimpleUpstream().getUid());
        heap.push(pipe.getUid(), transformer.transform(input), pipe.getDownstream().size());
        return true;
    }

    /**
     *
     * @param join
     * @param heap
     * @return
     * @throws TransformationException
     */
    @SuppressWarnings("unchecked")
    private boolean launchJoin(Join<?, ?, ?> join, Heap heap) throws TransformationException
    {
        BiTransformer transformer = join.getActor();

        logger.debug(
            "Launching flow join {}#{} of upstream flows {}#{} and {}#{}",
            getName(join),
            join.getUid(),
            getName(join.getUpstream1()),
            join.getUpstream1().getUid(),
            getName(join.getUpstream2()),
            join.getUpstream2().getUid()
        );

        Object input1 = heap.consume(join.getUpstream1().getUid());
        Object input2 = heap.consume(join.getUpstream2().getUid());
        heap.push(join.getUid(), transformer.transform(input1, input2), join.getDownstream().size());
        return true;
    }

    /**
     *
     * @param sink
     * @param heap
     * @return
     */
    @SuppressWarnings("unchecked")
    private boolean launchSink(Sink<?> sink, Heap heap) throws LoadingException
    {
        Loader loader = sink.getActor();

        logger.debug("Launching flow sink {}#{} of loader {}", getName(sink), sink.getUid(), loader.getClass().getName());

        Object input = heap.consume(sink.getSimpleUpstream().getUid());

        /* If the sink is a Recipient, the output value comes from the provided input instead of the extractor itself ; the extractor only holds a reference to the expected input */
        if (sink instanceof Recipient)
        {
            var identifier = ((Recipient) sink).getIdentifier();
            heap.setOutput(identifier, input);
        }
        else
            loader.load(input);

        return true;
    }

    /**
     *
     * @param offsetNode
     * @param heap
     * @return
     * @throws GenerationException
     * @throws TransformationException
     * @throws LoadingException
     * @throws PipelineRunException
     */
    private boolean launchOffset(OffsetNode offsetNode, Heap heap) throws GenerationException, TransformationException, LoadingException, PipelineRunException
    {
        Node node = offsetNode.getNode();
        int offset = offsetNode.getOffset();

        if (node instanceof StreamGenerator)
            return this.launchStreamGenerator((StreamGenerator<?, ?>) node, offset, heap);
        else if (node instanceof StreamPipe)
            return this.launchStreamPipe((StreamPipe<?, ?>) node, offset, heap);
        else if (node instanceof StreamJoin)
            return this.launchStreamJoin((StreamJoin<?, ?, ?>) node, offset, heap);
        else if (node instanceof StreamSink)
            return this.launchStreamSink((StreamSink<?>) node, offset, heap);

        logger.error("Flow node #{} is of an unknown {} type", node.getUid(), node.getClass().getName());

        throw new PipelineRunException("Unknown node type " + node.getClass().getName(), heap);
    }

    /**
     *
     * @param generatorNode
     * @param offset
     * @param heap
     * @return
     * @throws GenerationException
     */
    private boolean launchStreamGenerator(StreamGenerator<?, ?> generatorNode, int offset, Heap heap) throws GenerationException
    {
        Generator generator = heap.getStreamGenerator(generatorNode);

        logger.debug("Launching flow stream generator {}#{} at offset {} with generator {}", getName(generatorNode), generatorNode.getUid(), offset, generator.getClass().getName());

        heap.push(generatorNode.getUid(), offset, generator.generate(), generatorNode.getDownstream().size());
        return true;
    }

    /**
     *
     * @param pipe
     * @param offset
     * @param heap
     * @return
     * @throws TransformationException
     */
    @SuppressWarnings("unchecked")
    private boolean launchStreamPipe(StreamPipe<?, ?> pipe, int offset, Heap heap) throws TransformationException
    {
        Transformer transformer = pipe.getActor();

        logger.debug("Launching flow stream pipe {}#{} at offset {} of transformer {}", getName(pipe), pipe.getUid(), offset, transformer.getClass().getName());

        Object input = heap.consume(pipe.getSimpleUpstream().getUid(), offset);
        heap.push(pipe.getUid(), offset, transformer.transform(input), pipe.getDownstream().size());
        return true;
    }

    /**
     *
     * @param join
     * @param offset
     * @param heap
     * @return
     * @throws TransformationException
     */
    @SuppressWarnings("unchecked")
    private boolean launchStreamJoin(StreamJoin<?, ?, ?> join, int offset, Heap heap) throws TransformationException
    {
        BiTransformer transformer = join.getActor();

        logger.debug(
            "Launching flow stream join {}#{} at offset {} of upstream flows {}#{} and {}#{}",
            getName(join),
            join.getUid(),
            offset,
            getName(join.getUpstream1()),
            join.getUpstream1().getUid(),
            getName(join.getUpstream2()),
            join.getUpstream2().getUid()
        );

        Object input1 = heap.consume(join.getUpstream1().getUid(), offset);
        Object input2 = heap.consume(join.getUpstream2().getUid(), offset);
        heap.push(join.getUid(), offset, transformer.transform(input1, input2), join.getDownstream().size());
        return true;
    }

    /**
     *
     * @param sink
     * @param offset
     * @param heap
     * @return
     * @throws LoadingException
     */
    @SuppressWarnings("unchecked")
    private boolean launchStreamSink(StreamSink<?> sink, int offset, Heap heap) throws LoadingException
    {
        Loader loader = sink.getActor();

        logger.debug("Launching flow stream sink {}#{} at offset {} of loader {}", getName(sink), sink.getUid(), offset, loader.getClass().getName());

        Object input = heap.consume(sink.getSimpleUpstream().getUid(), offset);
        loader.load(input);
        return true;
    }

    /**
     *
     * @param node
     * @param heap
     * @return
     * @throws AccumulationException
     */
    @SuppressWarnings("unchecked")
    private boolean launchStreamAccumulator(StreamAccumulator<?, ?> node, Heap heap) throws AccumulationException
    {
        Accumulator accumulator = node.getActor();

        logger.debug("Launching flow stream accumulator {}#{} of accumulator {}", getName(node), node.getUid(), node.getClass().getName());

        Collection<Object> input = heap.consumeAll(node.getSimpleUpstream().getUid());
        heap.push(node.getUid(), accumulator.accumulate(input), node.getDownstream().size());
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
}
