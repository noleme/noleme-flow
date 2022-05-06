package com.noleme.flow.impl.parallel.runtime.heap;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.impl.parallel.runtime.state.RRWLock;
import com.noleme.flow.impl.pipeline.runtime.heap.Counter;
import com.noleme.flow.impl.pipeline.runtime.heap.Heap;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingKey;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingNode;
import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.output.OutputMap;
import com.noleme.flow.io.output.WriteableOutput;
import com.noleme.flow.stream.StreamGenerator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.noleme.flow.impl.pipeline.runtime.node.WorkingNode.upstreamOf;

/**
 * @author Pierre Lecerf (pierre.lecerf@gmail.com) on 23/07/15.
 */
@SuppressWarnings("rawtypes")
public class ConcurrentHashHeap implements Heap
{
    private final Map<WorkingKey, Counter> contents;
    private final Map<WorkingKey, Generator> generators;
    private final Map<WorkingKey, ConcurrentCounterContainer> streamContents;
    private final Map<WorkingKey, Long> offsets;
    private final Input input;
    private final WriteableOutput output;
    private final RRWLock contentLock = new RRWLock();
    private final RRWLock streamLock = new RRWLock();

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
    public Heap push(WorkingKey key, Object returnValue, int counter)
    {
        this.contents.put(key, new Counter(returnValue, counter));
        return this;
    }

    @Override
    public boolean has(WorkingKey key)
    {
        try {
            this.contentLock.read.lock();
            return this.contents.containsKey(key);
        }
        finally {
            this.contentLock.read.unlock();
        }
    }

    @Override
    public Object peek(WorkingKey key)
    {
        try {
            this.contentLock.read.lock();

            if (this.contents.containsKey(key))
                return this.contents.get(key).getValue();
            return null;
        }
        finally {
            this.contentLock.read.unlock();
        }
    }

    @Override
    public Object consume(WorkingKey key)
    {
        try {
            this.contentLock.write.lock();

            if (this.contents.containsKey(key))
            {
                Counter counter = this.contents.get(key).decrement();
                if (counter.getCount() == 0)
                    this.contents.remove(key);
                return counter.getValue();
            }
            return null;
        }
        finally {
            this.contentLock.write.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    synchronized public Generator getStreamGenerator(WorkingNode<StreamGenerator> node)
    {
        if (!this.generators.containsKey(node.getKey()))
        {
            /* If the node has an upstream node, we recover its output, otherwise the generator has a null input */
            var argument = !node.getUpstream().isEmpty()
                ? this.consume(upstreamOf(node).getKey()) //TODO: account for offsets
                : null
            ;

            this.generators.put(node.getKey(), node.getNode().produceGenerator(argument));
        }
        return this.generators.get(node.getKey());
    }

    @Override
    synchronized public long getNextStreamOffset(WorkingNode<StreamGenerator> node)
    {
        this.offsets.put(node.getKey(), this.offsets.getOrDefault(node.getKey(), -1L) + 1);
        return this.offsets.get(node.getKey());
    }

    @Override
    public Collection<Object> peekAll(WorkingKey key)
    {
        try {
            this.streamLock.write.lock();

            if (!this.streamContents.containsKey(key))
                return Collections.emptyList();

            return this.streamContents.get(key).stream()
                .map(Counter::getValue)
                .collect(Collectors.toList())
            ;
        }
        finally {
            this.streamLock.write.unlock();
        }
    }

    @Override
    public Collection<Object> consumeAll(WorkingKey key)
    {
        try {
            this.streamLock.write.lock();

            if (!this.streamContents.containsKey(key))
                return Collections.emptyList();

            List<Object> values = this.streamContents.get(key).stream()
                .map(c -> c.decrement().getValue())
                .collect(Collectors.toList())
            ;

            if (this.streamContents.get(key).removeConsumed() == 0)
                this.streamContents.remove(key);

            return values;
        }
        finally {
            this.streamLock.write.unlock();
        }
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
    public WriteableOutput getOutput()
    {
        return this.output;
    }

    /**
     *
     * @param key
     * @return
     */
    private boolean hasStreamContent(WorkingKey key)
    {
        var container = this.streamContents.get(key);

        return container != null
            && container.get(key.offset()) != null
        ;
    }

    @Override
    public String dump() {
        var sb = new StringBuilder();

        sb.append(this).append("\n");
        sb.append("  contentlock:\n");
        sb.append("    read: ").append(this.contentLock.read).append("\n");
        sb.append("    write: ").append(this.contentLock.write).append("\n");
        sb.append("  streamlock:\n");
        sb.append("    read: ").append(this.streamLock.read).append("\n");
        sb.append("    write: ").append(this.streamLock.write).append("\n");
        sb.append("  contents:\n");
        this.contents.forEach((uid, counter) -> {
            sb.append("    ").append(uid).append(": ").append(counter.getValue()).append(" (count=").append(counter.getCount()).append(")\n");
        });
        sb.append("  streams:\n");
        sb.append("    generators:\n");
        this.generators.forEach((uid, gen) -> {
            sb.append("      ").append(uid).append(": ").append(gen).append(" (hasNext=").append(gen.hasNext()).append(")\n");
        });
        sb.append("    offsets:\n");
        this.offsets.forEach((uid, offset) -> {
            sb.append("      ").append(uid).append(": ").append(offset).append("\n");
        });
        sb.append("    contents:\n");
        this.streamContents.forEach((uid, container) -> {
            sb.append("      ").append(uid).append(":\n");
            container.stream().forEach(counter -> {
                sb.append("        ").append(counter.getValue()).append(" (count=").append(counter.getCount()).append(")\n");
            });
        });

        return sb.toString();
    }
}
