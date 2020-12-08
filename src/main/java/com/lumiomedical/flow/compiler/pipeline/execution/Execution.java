package com.lumiomedical.flow.compiler.pipeline.execution;

import com.lumiomedical.flow.actor.extractor.ExtractionException;
import com.lumiomedical.flow.actor.extractor.Extractor;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.loader.LoadingException;
import com.lumiomedical.flow.Join;
import com.lumiomedical.flow.Pipe;
import com.lumiomedical.flow.Sink;
import com.lumiomedical.flow.Source;
import com.lumiomedical.flow.compiler.pipeline.PipelineRunException;
import com.lumiomedical.flow.compiler.pipeline.heap.Heap;
import com.lumiomedical.flow.logger.Logging;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.TransformationException;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.stream.StreamJoin;
import com.lumiomedical.flow.stream.StreamPipe;
import com.lumiomedical.flow.stream.StreamSink;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
public class Execution
{
    /**
     * Actually executes the Node passed as parameter.
     * The method is responsible for :
     * - extracting the node's input parameters from the Heap
     * - identifying the node's method signature
     * - upon success: push the return values to the Heap
     * - upon failure: either abort the Node, stop or abort the Runtime
     *
     * Note that in the current implementation, only "blocking" errors are handled, resulting in a complete exit from the pipe.
     * Returning false was designed as a mean for a "soft exit" which results in blocking downstream paths while continuing on running paths that are still valid.
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
                return this.launchJoin((Join) node, heap);
            else if (node instanceof Sink)
                return this.launchSink((Sink<?>) node, heap);
            else if (node instanceof StreamPipe)
                return this.launchStreamPipe((StreamPipe<?, ?>) node, heap);
            else if (node instanceof StreamJoin)
                return this.launchStreamJoin((StreamJoin) node, heap);
            else if (node instanceof StreamSink)
                return this.launchStreamSink((StreamSink<?>) node, heap);

            /*
             * Returning false is a "silent" failure mode, which can be used to signify a no-go for downstream node without stopping the rest of the graph execution.
             * Here we really want to crash the whole party since we apparently have an unknown node subtype.
             */
            Logging.logger.error("Flow node #"+node.getUid()+" is of an unknown "+node.getClass().getName()+" type");

            throw new PipelineRunException("Unknown node type " + node.getClass().getName(), heap);
        }
        catch (ExtractionException | TransformationException | LoadingException e) {
            throw new PipelineRunException(
                "Node " + node.getClass().getName() + "#" + node.getUid() + " has thrown an exception. (" + e.getClass() + ")", e, heap
            );
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

        Logging.logger.debug("Launching flow source #"+source.getUid()+" of extractor "+extractor.getClass().getName());

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

        Logging.logger.debug("Launching flow pipe #"+pipe.getUid()+" of transformer "+transformer.getClass().getName());

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
    private boolean launchJoin(Join join, Heap heap) throws TransformationException
    {
        BiTransformer transformer = join.getActor();

        Logging.logger.debug("Launching flow join #"+join.getUid()+" of upstream flows #"+join.getUpstream1().getUid()+" and #"+join.getUpstream2().getUid());

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

        Logging.logger.debug("Launching flow sink #"+sink.getUid()+" of loader "+loader.getClass().getName());

        Object input = heap.consume(sink.getSimpleUpstream().getUid());
        loader.load(input);
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
    private boolean launchStreamPipe(StreamPipe<?, ?> pipe, Heap heap) throws TransformationException
    {
        Transformer transformer = pipe.getActor();

        Logging.logger.debug("Launching flow stream pipe #"+pipe.getUid()+" of transformer "+transformer.getClass().getName());

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
    private boolean launchStreamJoin(StreamJoin join, Heap heap) throws TransformationException
    {
        BiTransformer transformer = join.getActor();

        Logging.logger.debug("Launching flow stream join #"+join.getUid()+" of upstream flows #"+join.getUpstream1().getUid()+" and #"+join.getUpstream2().getUid());

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
    private boolean launchStreamSink(StreamSink<?> sink, Heap heap) throws LoadingException
    {
        Loader loader = sink.getActor();

        Logging.logger.debug("Launching flow stream sink #"+sink.getUid()+" of loader "+loader.getClass().getName());

        Object input = heap.consume(sink.getSimpleUpstream().getUid());
        loader.load(input);
        return true;
    }
}
