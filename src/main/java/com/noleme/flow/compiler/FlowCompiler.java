package com.noleme.flow.compiler;

import com.noleme.flow.node.Node;

import java.util.Arrays;
import java.util.Collection;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public interface FlowCompiler<R extends FlowRuntime>
{
    /**
     *
     * @param inputNodes
     * @return
     * @throws CompilationException
     */
    R compile(Collection<Node> inputNodes) throws CompilationException;

    /**
     *
     * @param inputNodes
     * @return
     * @throws CompilationException
     */
    default R compile(Node... inputNodes) throws CompilationException
    {
        return this.compile(Arrays.asList(inputNodes));
    }
}
