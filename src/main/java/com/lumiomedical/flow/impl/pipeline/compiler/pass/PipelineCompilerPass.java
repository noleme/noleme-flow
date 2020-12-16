package com.lumiomedical.flow.impl.pipeline.compiler.pass;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.node.Node;

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
