package com.noleme.flow.impl.pipeline.runtime.heap;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.input.Key;
import com.noleme.flow.io.output.OutputMap;
import com.noleme.flow.io.output.WriteableOutput;
import com.noleme.flow.stream.StreamGenerator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (pierre.lecerf@gmail.com) on 23/01/15.
 */
public class HashHeap implements Heap
{
    private final Map<String, Counter> contents;
    private final Map<String, Generator<?>> generators;
    private final Map<String, CounterContainer> streamContents;
    private final Map<String, Long> offsets;
    private final Input input;
    private final WriteableOutput output;

    public HashHeap(Input input)
    {
        super();
        this.contents = new HashMap<>();
        this.streamContents = new HashMap<>();
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
    public Object peek(String id)
    {
        if (this.contents.containsKey(id))
            return this.contents.get(id).getValue();
        return null;
    }

    @Override
    public Object consume(String id)
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
    public Generator getStreamGenerator(StreamGenerator node)
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
    public long getNextStreamOffset(StreamGenerator node)
    {
        this.offsets.put(node.getUid(), this.offsets.getOrDefault(node.getUid(), -1L) + 1);
        return this.offsets.get(node.getUid());
    }

    @Override
    public Heap push(String id, long offset, Object returnValue, int counter)
    {
        if (!this.streamContents.containsKey(id))
            this.streamContents.put(id, new CounterContainer());

        this.streamContents.get(id).set(offset, new Counter(returnValue, counter));

        return this;
    }

    @Override
    public boolean has(String id, long offset)
    {
        return this.hasStreamContent(id, offset);
    }

    @Override
    public Object peek(String id, long offset)
    {
        if (this.hasStreamContent(id, offset))
            return this.streamContents.get(id).get(offset).getValue();
        else if (this.has(id))
            return this.contents.get(id).getValue();
        return null;
    }

    @Override
    public Object consume(String id, long offset)
    {
        if (this.hasStreamContent(id, offset))
        {
            CounterContainer container = this.streamContents.get(id);
            Counter counter = container.get(offset).decrement();

            if (counter.getCount() == 0)
                container.remove(offset);

            return counter.getValue();
        }
        else if (this.has(id))
            return this.contents.get(id).decrement().getValue();
        return null;
    }

    @Override
    public Collection<Object> consumeAll(String id)
    {
        if (!this.streamContents.containsKey(id))
            return Collections.emptyList();

        List<Object> values = this.streamContents.get(id).stream()
            .map(c -> c.decrement().getValue())
            .collect(Collectors.toList())
        ;

        if (this.streamContents.get(id).removeConsumed() == 0)
            this.streamContents.remove(id);

        return values;
    }

    @Override
    public boolean hasInput(Key<?> key)
    {
        return this.input.has(key);
    }

    @Override
    public <T> T getInput(Key<T> key)
    {
        return this.input.get(key);
    }

    @Override
    public Heap setOutput(String identifier, Object value)
    {
        this.output.set(identifier, value);
        return this;
    }

    @Override
    public WriteableOutput getOutput()
    {
        return this.output;
    }

    /**
     *
     * @param id
     * @param offset
     * @return
     */
    private boolean hasStreamContent(String id, long offset)
    {
        var container = this.streamContents.get(id);

        return container != null
            && container.get(offset) != null
        ;
    }
}
