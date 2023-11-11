package com.noleme.flow.impl.pipeline.compiler.pass;

import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.impl.pipeline.compiler.stream.StreamPipeline;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamAccumulator;
import com.noleme.flow.stream.StreamGenerator;
import com.noleme.flow.stream.StreamNode;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/13
 */
public class StreamAggregationPass implements PipelineCompilerPass
{
    @Override
    public Collection<Node> run(Collection<Node> nodes) throws CompilationException
    {
        Registry registry = new Registry();

        LinkedList<Node> queue = new LinkedList<>(nodes);

        while (!queue.isEmpty())
        {
            Node node = queue.poll();

            if (registry.contains(node) || registry.isPartOfPipeline(node))
                continue;
            if (node instanceof StreamGenerator)
            {
                var pipeline = new StreamPipeline((StreamGenerator<?, ?>) node);
                registry.registerPipelinePart(pipeline, node);
                this.compileStream(node, pipeline, registry);
                queue.push(pipeline);
            }
            else if (node instanceof StreamPipeline)
            {
                if (registry.isPipelineUpstreamSatisfied((StreamPipeline) node))
                    registry.add(node);
                else
                    queue.add(node);
            }
            else {
                if (registry.isUpstreamSatisfied(node))
                    registry.add(node);
                else
                    queue.add(node);
            }
        }

        return registry.nodes();
    }

    private void compileStream(Node node, StreamPipeline pipeline, Registry registry)
    {
        for (Node usn : node.getUpstream())
        {
            if (usn instanceof StreamNode || usn instanceof StreamGenerator || usn instanceof StreamAccumulator)
                continue;
            registry.registerPivot(pipeline.getTopParent(), usn);
        }
        for (Node dsn : node.getDownstream())
        {
            /* If we find a cascading stream, we initialize a sub-stream pipeline and crawl it */
            if (dsn instanceof StreamGenerator)
            {
                var sub = new StreamPipeline((StreamGenerator<?, ?>) dsn, pipeline);
                pipeline.add(sub);
                registry.registerPipelinePart(pipeline, dsn);
                registry.registerPipelinePart(pipeline, sub);
                this.compileStream(dsn, sub, registry);
            }
            /* If we find stream nodes, we push them to the pipeline */
            else if (dsn instanceof StreamNode)
            {
                pipeline.add(dsn);
                registry.registerPipelinePart(pipeline, dsn);
                this.compileStream(dsn, pipeline, registry);
            }
            //else if (dsn instanceof StreamAccumulator && pipeline.getParent() != null)
            else if (dsn instanceof StreamAccumulator)
            {
                //pipeline.getParent().add(dsn);
                pipeline.add(dsn);
                //registry.registerPipelinePart(pipeline.getParent(), dsn);
                registry.registerPipelinePart(pipeline, dsn);
                //this.compileStream(dsn, pipeline.getParent(), registry);
                this.compileStream(dsn, pipeline.getParent(), registry);
            }
        }
        //registry.register(pipeline);
    }

    private static class Registry
    {
        private final Set<Node> registry = new HashSet<>();
        private final List<Node> nodes = new ArrayList<>();
        private final Map<StreamPipeline, Set<Node>> inPipelineMap = new HashMap<>();
        private final Set<Node> inPipeline = new HashSet<>();
        private final Map<StreamPipeline, Set<Node>> pivots = new HashMap<>();

        public Registry add(Node node)
        {
            this.register(node);
            this.nodes.add(node);
            return this;
        }

        public Registry register(Node node)
        {
            this.registry.add(node);

            if (node instanceof StreamPipeline)
            {
                this.register(((StreamPipeline) node).getGeneratorNode());
                for (Node dsn : ((StreamPipeline)node).getNodes())
                    this.register(dsn);
            }

            return this;
        }

        public Registry registerPivot(StreamPipeline pipeline, Node node)
        {
            if (!this.pivots.containsKey(pipeline))
                this.pivots.put(pipeline, new HashSet<>());
            this.pivots.get(pipeline).add(node);
            return this;
        }

        public Registry registerPipelinePart(StreamPipeline pipeline, Node node)
        {
            if (!this.inPipelineMap.containsKey(pipeline))
                this.inPipelineMap.put(pipeline, new HashSet<>());
            this.inPipelineMap.get(pipeline).add(node);
            this.inPipeline.add(node);
            return this;
        }

        public boolean contains(Node node)
        {
            return this.registry.contains(node);
        }

        public boolean isPartOfPipeline(Node node)
        {
            return this.inPipeline.contains(node);
        }

        public boolean isPartOfPipeline(StreamPipeline pipeline, Node node)
        {
            if (this.inPipelineMap.containsKey(pipeline))
                return false;
            return this.inPipelineMap.get(pipeline).contains(node);
        }

        public boolean isPipelineUpstreamSatisfied(StreamPipeline pipeline)
        {
            /* If the pipeline cannot be found in the registry, as far as the registry is concerned its upstream is satisfied */
            if (!this.pivots.containsKey(pipeline))
                return true;

            long pivotNotSatisfied = this.pivots.get(pipeline).stream()
                .filter(pn -> !this.contains(pn) && !this.isPartOfPipeline(pipeline, pn))
                .count()
            ;

            long upstreamNotSatisfied = pipeline.getUpstream().stream()
                .filter(usn -> !this.contains(usn) && !this.isPartOfPipeline(pipeline, usn))
                .count()
            ;

            return pivotNotSatisfied + upstreamNotSatisfied == 0;
        }

        public boolean isUpstreamSatisfied(Node node)
        {
            if (node.getUpstream().isEmpty())
                return true;

            long notSatisfied = node.getUpstream().stream()
                .filter(usn -> !this.registry.contains(usn))
                .count()
            ;

            return notSatisfied == 0;
        }

        public List<Node> nodes()
        {
            return this.nodes;
        }
    }
}
