package com.lumiomedical.flow.impl.pipeline;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.FlowCompiler;
import com.lumiomedical.flow.impl.pipeline.compiler.pass.PipelineCompilerPass;
import com.lumiomedical.flow.impl.pipeline.compiler.pass.StreamAggregationPass;
import com.lumiomedical.flow.impl.pipeline.compiler.pass.TopologicalSortPass;
import com.lumiomedical.flow.node.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class PipelineCompiler implements FlowCompiler<PipelineRuntime>
{
    private final List<PipelineCompilerPass> passes = List.of(
        new TopologicalSortPass(),
        new StreamAggregationPass()
    );

    @Override
    public PipelineRuntime compile(Collection<Node> inputNodes) throws CompilationException
    {
        List<Node> compiledNodes = compile(inputNodes, this.passes);

        return new PipelineRuntime(compiledNodes);
    }

    /**
     *
     * @param nodes
     * @param passes
     * @return
     * @throws CompilationException
     */
    public static List<Node> compile(Collection<Node> nodes, List<PipelineCompilerPass> passes) throws CompilationException
    {
        for (PipelineCompilerPass pass : passes)
            nodes = pass.run(nodes);

        return nodes instanceof List
            ? (List<Node>) nodes
            : new ArrayList<>(nodes)
        ;
    }
}
