package com.lumiomedical.flow.impl.parallel.compiler.pass;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.impl.pipeline.compiler.pass.PipelineCompilerPass;
import com.lumiomedical.flow.node.Node;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/13
 */
public class RemoveNodesWithUpstreamPass implements PipelineCompilerPass
{
    @Override
    public Collection<Node> run(Collection<Node> nodes) throws CompilationException
    {
        return nodes.stream()
            .filter(n -> n.getUpstream().isEmpty())
            .collect(Collectors.toList())
        ;
    }
}
