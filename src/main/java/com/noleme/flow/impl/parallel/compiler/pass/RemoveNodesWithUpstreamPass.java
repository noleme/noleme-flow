package com.noleme.flow.impl.parallel.compiler.pass;

import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.impl.pipeline.compiler.pass.PipelineCompilerPass;
import com.noleme.flow.node.Node;

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
