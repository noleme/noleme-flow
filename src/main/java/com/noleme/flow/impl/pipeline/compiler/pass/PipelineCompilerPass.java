package com.noleme.flow.impl.pipeline.compiler.pass;

import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.node.Node;

import java.util.Collection;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/13
 */
public interface PipelineCompilerPass
{
    /**
     *
     * @param nodes
     * @return
     * @throws CompilationException
     */
    Collection<Node> run(Collection<Node> nodes) throws CompilationException;
}
