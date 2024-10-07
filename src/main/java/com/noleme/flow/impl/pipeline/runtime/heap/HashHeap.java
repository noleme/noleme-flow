package com.noleme.flow.impl.pipeline.runtime.heap;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingKey;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingNode;
import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.input.Key;
import com.noleme.flow.io.output.OutputMap;
import com.noleme.flow.io.output.WriteableOutput;
import com.noleme.flow.stream.StreamGenerator;

import java.util.*;
import java.util.stream.Collectors;

import static com.noleme.flow.impl.pipeline.runtime.node.WorkingNode.upstreamOf;

/**
 * @author Pierre Lecerf (pierre.lecerf@gmail.com) on 23/01/15.
 */
public class HashHeap implements Heap
{
    private final Map<WorkingKey, Counter> contents;
    private final Map<WorkingKey, Generator<?>> generators;
    private final Map<WorkingKey, Set<WorkingKey>> offsetKeys;
    private final Map<WorkingKey, Long> offsets;
    private final Input input;
    private final WriteableOutput output;

    public HashHeap(Input input)
    {
        super();
        this.contents = new HashMap<>();
        this.offsetKeys = new HashMap<>();
        this.generators = new HashMap<>();
        this.offsets = new HashMap<>();
        this.input = input;
        this.output = new OutputMap();
    }

    @Override
    public Heap push(WorkingKey key, Object returnValue, int counter)
    {
        this.contents.put(key, new Counter(returnValue, counter));
        if (key.hasOffset())
        {
            var keyWithoutOffset = key.withoutOffset();

            if (!this.offsetKeys.containsKey(keyWithoutOffset))
                this.offsetKeys.put(keyWithoutOffset, new HashSet<>());
            this.offsetKeys.get(keyWithoutOffset).add(key);
        }
        return this;
    }

    @Override
    public boolean has(WorkingKey key)
    {
        return this.contents.containsKey(key);
    }

    @Override
    public Object peek(WorkingKey key)
    {
        if (this.contents.containsKey(key))
            return this.contents.get(key).getValue();
        return null;
    }

    @Override
    public Object consume(WorkingKey key)
    {
        if (this.contents.containsKey(key))
        {
            Counter counter = this.contents.get(key).decrement();
            if (counter.getCount() == 0)
                this.contents.remove(key);
            return counter.getValue();
        }
        return null;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Generator<?> getStreamGenerator(WorkingNode<StreamGenerator> node)
    {
        WorkingKey key = node.getKey().hasOffset()
            ? node.getKey().withoutOffset()
            : node.getKey()
        ;

        if (!this.generators.containsKey(key))
        {
            /* If the node has an upstream node, we recover its output, otherwise the generator has a null input */
            Object argument = !node.getUpstream().isEmpty()
                ? this.consume(upstreamOf(node).getKey())
                : null
            ;

            this.generators.put(key, node.getNode().produceGenerator(argument));
        }
        return this.generators.get(key);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public long getNextStreamOffset(WorkingNode<StreamGenerator> node)
    {
        this.offsets.put(node.getKey(), this.offsets.getOrDefault(node.getKey(), -1L) + 1);
        return this.offsets.get(node.getKey());
    }

    @Override
    public Collection<Object> peekAll(WorkingKey key)
    {
        if (!this.offsetKeys.containsKey(key))
            return Collections.emptyList();

        return this.offsetKeys.get(key).stream()
            .sorted((k1, k2) -> (int) (k1.offset() - k2.offset()))
            .map(this::peek)
            .collect(Collectors.toList())
        ;
    }

    @Override
    public Collection<Object> consumeAll(WorkingKey key)
    {
        if (!this.offsetKeys.containsKey(key))
            return Collections.emptyList();

        return this.offsetKeys.get(key).stream()
            .sorted((k1, k2) -> (int) (k1.offset() - k2.offset()))
            .map(this::consume)
            .collect(Collectors.toList())
        ;
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
}
