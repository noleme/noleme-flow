package com.noleme.flow.stream;

import com.noleme.flow.*;
import com.noleme.flow.actor.accumulator.Accumulator;
import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.Beam;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.annotation.Experimental;
import com.noleme.flow.node.SimpleNode;

import java.util.function.Function;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public class StreamAccumulator<I, O> extends SimpleNode<Accumulator<I, O>> implements StreamIn<I>, CurrentOut<O>
{
    /**
     * @param actor
     */
    public StreamAccumulator(Accumulator<I, O> actor, int depth)
    {
        super(actor);
        this.setDepth(depth);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <NO> CurrentOut<NO> into(Transformer<O, NO> transformer)
    {
        CurrentOut<NO> pipe = this.getDepth() > 0
            ? new StreamPipe<>(transformer, this.depth)
            : new Pipe<>(transformer)
        ;
        this.bind((SimpleNode) pipe);
        return pipe;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CurrentIn<O> into(Loader<O> loader)
    {
        CurrentIn<O> sink = this.getDepth() > 0
            ? new StreamSink<>(loader, this.depth)
            : new Sink<>(loader)
        ;
        this.bind((SimpleNode) sink);
        return sink;
    }

    @Override
    public <JI, JO> CurrentOut<JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer)
    {
        return this.getDepth() > 0
            ? new StreamJoin<>(this, input, transformer, this.depth)
            : new Join<>(this, input, transformer)
        ;
    }

    @Override
    public <N> StreamGenerator<O, N> stream(Function<O, Generator<N>> generatorSupplier)
    {
        var pipe = new StreamGenerator<>(generatorSupplier, this.depth + 1);
        this.bind(pipe);
        return pipe;
    }

    /**
     *
     * @param loader
     * @return
     */
    public StreamAccumulator<I, O> driftSink(Loader<O> loader)
    {
        this.into(loader);
        return this;
    }

    /**
     * StreamAccumulator is a CurrentOut, depending on whether the upstream is part of a cascading stream or not, it can be either flowing into a FlowIn or a StreamIn.
     * As a result, {@link CurrentOut#asStream()} needs to be overridden to introduce a technical node in between, which itself will have a determined type.
     */
    @Override
    @Experimental
    public StreamOut<O> asStream()
    {
        return this.pipe(new Beam<>()).asStream();
    }

    /**
     * StreamAccumulator is a CurrentOut, depending on whether the upstream is part of a cascading stream or not, it can be either flowing into a FlowIn or a StreamIn.
     * As a result, {@link CurrentOut#asFlow()} needs to be overridden to introduce a technical node in between, which itself will have a determined type.
     */
    @Override
    @Experimental
    public FlowOut<O> asFlow()
    {
        return this.pipe(new Beam<>()).asFlow();
    }

    /**
     *
     * @param name
     * @return
     */
    public StreamAccumulator<I, O> name(String name)
    {
        this.name = name;
        return this;
    }
}
