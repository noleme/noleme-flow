package com.lumiomedical.flow;

import com.lumiomedical.flow.actor.extractor.Extractor;
import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.node.SimpleNode;
import com.lumiomedical.flow.recipient.Recipient;
import com.lumiomedical.flow.stream.StreamGenerator;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Sources represent an entrypoint node in a DAG.
 * They have no upstream but can propagate their output downstream.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/28
 */
public class Source<O> extends SimpleNode<Extractor<O>> implements FlowOut<O>
{
    /**
     *
     * @param actor
     */
    public Source(Extractor<O> actor)
    {
        super(actor);
    }

    @Override
    public <N> Pipe<O, N> into(Transformer<O, N> transformer)
    {
        var pipe = new Pipe<>(transformer);
        this.bind(pipe);
        return pipe;
    }

    @Override
    public Sink<O> into(Loader<O> loader)
    {
        var sink = new Sink<>(loader);
        this.bind(sink);
        return sink;
    }

    @Override
    public <JI, JO> Join<O, JI, JO> join(FlowOut<JI> input, BiTransformer<O, JI, JO> transformer)
    {
        return new Join<>(this, input, transformer);
    }

    @Override
    public <N> StreamGenerator<O, N> stream(Function<O, Generator<N>> generatorSupplier)
    {
        var pipe = new StreamGenerator<>(generatorSupplier);
        this.bind(pipe);
        return pipe;
    }

    /**
     *
     * @param loader
     * @return
     */
    public Source<O> drift(Loader<O> loader)
    {
        this.into(loader);
        return this;
    }

    @Override
    public Recipient<O> collect(String name)
    {
        Recipient<O> recipient = new Recipient<>(name);
        this.bind(recipient);
        return recipient;
    }

    /**
     *
     * @param name
     * @return
     */
    public Source<O> sample(String name)
    {
        this.collect(name);
        return this;
    }

    @Override
    public List<Node> getUpstream()
    {
        return Collections.emptyList();
    }
}
