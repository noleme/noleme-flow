package com.noleme.flow.impl.parallel.runtime.heap;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.impl.pipeline.runtime.heap.CounterContainer;
import com.noleme.flow.impl.pipeline.runtime.heap.Counter;
import com.noleme.flow.impl.pipeline.runtime.heap.Heap;
import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.output.Output;
import com.noleme.flow.io.output.OutputMap;
import com.noleme.flow.io.output.WriteableOutput;
import com.noleme.flow.stream.StreamGenerator;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (pierre.lecerf@gmail.com) on 23/07/15.
 */
public class ConcurrentHashHeap implements Heap
{
    private final Map<String, Counter> contents;
    private final Map<String, Generator<?>> generators;
    private final Map<String, CounterContainer> streamContents;
    private final Map<String, Integer> offsets;
    private final Input input;
    private final WriteableOutput output;

    public ConcurrentHashHeap(Input input)
    {
        super();
        this.contents = new ConcurrentHashMap<>();
        this.streamContents = new ConcurrentHashMap<>();
        this.generators = new HashMap<>();
        this.offsets = new HashMap<>();
        this.input = input;
        this.output = new OutputMap();
    }

    @Override
    public Heap push(String id, Object returnValue, int counter)
    {
        this.contents.put(id, new Counter(returnValue, counter));
        return this;
    }

    @Override
    public boolean has(String id)
    {
        return this.contents.containsKey(id);
    }

    @Override
    synchronized public Object peek(String id)
    {
        if (this.contents.containsKey(id))
            return this.contents.get(id).getValue();
        return null;
    }

    @Override
    synchronized public Object consume(String id)
    {
        if (this.contents.containsKey(id))
        {
            Counter counter = this.contents.get(id).decrement();
            if (counter.getCount() == 0)
                this.contents.remove(id);
            return counter.getValue();
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    synchronized public Generator getStreamGenerator(StreamGenerator node)
    {
        if (!this.generators.containsKey(node.getUid()))
        {
            /* If the node has an upstream node, we recover its output, otherwise the generator has a null input */
            var argument = node.getSimpleUpstream() != null
                ? this.consume(node.getSimpleUpstream().getUid())
                : null
            ;

            this.generators.put(node.getUid(), node.produceGenerator(argument));
        }
        return this.generators.get(node.getUid());
    }

    @Override
    synchronized public int getNextStreamOffset(StreamGenerator node)
    {
        this.offsets.put(node.getUid(), this.offsets.getOrDefault(node.getUid(), -1) + 1);
        return this.offsets.get(node.getUid());
    }

    @Override
    synchronized public Heap push(String id, int offset, Object returnValue, int counter)
    {
        if (!this.streamContents.containsKey(id))
            this.streamContents.put(id, new CounterContainer());

        this.streamContents.get(id).set(offset, new Counter(returnValue, counter));

        return this;
    }

    @Override
    public boolean has(String id, int offset)
    {
        if (this.hasStreamContent(id, offset))
            return true;
        return this.has(id);
    }

    @Override
    public Object peek(String id, int offset)
    {
        if (this.hasStreamContent(id, offset))
            return this.streamContents.get(id).get(offset).getValue();
        else if (this.has(id))
            return this.contents.get(id).getValue();
        return null;
    }

    @Override
    synchronized public Object consume(String id, int offset)
    {
        if (this.hasStreamContent(id, offset))
            return this.streamContents.get(id).get(offset).decrement().getValue();
        else if (this.has(id))
            return this.contents.get(id).decrement().getValue();
        return null;
    }

    @Override
    synchronized public Collection<Object> consumeAll(String id)
    {
        if (this.streamContents.containsKey(id))
        {
            List<Object> values = this.streamContents.get(id).stream()
                .map(c -> c.decrement().getValue())
                .collect(Collectors.toList())
            ;

            this.streamContents.get(id).removeConsumed();

            return values;
        }

        return null;
    }

    @Override
    public boolean hasInput(String identifier)
    {
        return this.input.has(identifier);
    }

    @Override
    public Object getInput(String identifier)
    {
        return this.input.get(identifier);
    }

    @Override
    synchronized public Heap setOutput(String identifier, Object value)
    {
        this.output.set(identifier, value);
        return this;
    }

    @Override
    public Output getOutput()
    {
        return this.output;
    }

    /**
     *
     * @param id
     * @param offset
     * @return
     */
    private boolean hasStreamContent(String id, int offset)
    {
        var container = this.streamContents.get(id);

        return container != null
            && container.get(offset) != null
        ;
    }
}
