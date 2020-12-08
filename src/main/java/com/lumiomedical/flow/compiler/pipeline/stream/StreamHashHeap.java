package com.lumiomedical.flow.compiler.pipeline.stream;

import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.compiler.pipeline.heap.Counter;
import com.lumiomedical.flow.compiler.pipeline.heap.Heap;
import com.lumiomedical.flow.node.Node;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/06
 */
public class StreamHashHeap implements StreamHeap
{
    private final Heap backend;
    private final Map<String, Generator<?>> generators;
    private final Map<String, ArrayList<Counter>> contents;
    private final Map<String, Integer> offsets;

    public StreamHashHeap(Heap heap)
    {
        this.backend = heap;
        this.generators = new HashMap<>();
        this.contents = new HashMap<>();
        this.offsets = new HashMap<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Generator getGenerator(StreamPipelineNode streamNode, Heap heap)
    {
        if (!this.generators.containsKey(streamNode.getUid()))
        {
            var generatorNode = streamNode.getGeneratorNode();
            var argument = heap.consume(generatorNode.getSimpleUpstream().getUid());
            this.generators.put(streamNode.getUid(), generatorNode.produceGenerator(argument));
        }
        return this.generators.get(streamNode.getUid());
    }

    @Override
    public int incrementOffset(Node node)
    {
        this.offsets.put(node.getUid(), this.offsets.getOrDefault(node.getUid(), -1) + 1);
        return this.offsets.get(node.getUid());
    }

    @Override
    public boolean hasOffset(Node node)
    {
        return this.offsets.containsKey(node.getUid());
    }

    @Override
    public StreamHeap registerOffset(Collection<Node> nodes, int offset)
    {
        for (Node node : nodes)
            this.offsets.put(node.getUid(), offset);
        return this;
    }

    @Override
    public Heap push(String id, Object returnValue, int counter)
    {
        if (!this.offsets.containsKey(id))
            return this.backend.push(id, returnValue, counter);
        
        if (!this.contents.containsKey(id))
            this.contents.put(id, new ArrayList<>());

        int offset = this.offsets.get(id);

        if (this.contents.get(id).size() <= offset)
            this.contents.get(id).add(null);

        this.contents.get(id).set(offset, new Counter(returnValue, counter));
        return this;
    }

    @Override
    public boolean has(String id)
    {
        return this.hasContent(id) || this.backend.has(id);
    }

    @Override
    public Object peek(String id)
    {
        if (this.hasContent(id))
            return this.contents.get(id).get(this.offsets.get(id)).getValue();
        else if (this.backend.has(id))
            return this.backend.peek(id);
        return null;
    }

    @Override
    public Object consume(String id)
    {
        if (this.hasContent(id))
        {
            int offset = this.offsets.get(id);
            Counter counter = this.contents.get(id).get(offset).decrement();
            if (counter.getCount() == 0)
                this.contents.get(id).set(offset, null);
            return counter.getValue();
        }
        else if (this.backend.has(id))
            return this.backend.peek(id);

        return null;
    }

    @Override
    public Collection<Object> consumeAll(String id)
    {
        if (this.hasContent(id))
        {
            List<Counter> counters = this.contents.get(id);

            Collection<Object> values = counters.stream()
                .map(c -> c.decrement().getValue())
                .collect(Collectors.toList())
            ;

            for (int offset = 0 ; offset < counters.size() ; ++offset)
            {
                if (counters.get(offset).getCount() == 0)
                    counters.set(offset, null);
            }

            return values;
        }

        return null;
    }

    /**
     *
     * @param id
     * @return
     */
    private boolean hasContent(String id)
    {
        return this.contents.containsKey(id)
            && this.contents.get(id).get(this.offsets.get(id)) != null
        ;
    }
}
