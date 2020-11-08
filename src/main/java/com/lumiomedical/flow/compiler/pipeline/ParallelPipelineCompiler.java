package com.lumiomedical.flow.compiler.pipeline;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.FlowCompiler;
import com.lumiomedical.flow.compiler.pipeline.parallel.ExecutorServiceProvider;
import com.lumiomedical.flow.compiler.pipeline.parallel.Executors;
import com.lumiomedical.flow.node.Node;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
public class ParallelPipelineCompiler implements FlowCompiler<ParallelPipelineRuntime>
{
    private final ExecutorServiceProvider provider;
    private boolean autoRefresh;

    /**
     *
     */
    public ParallelPipelineCompiler()
    {
        this(Runtime.getRuntime().availableProcessors(), false);
    }

    /**
     *
     */
    public ParallelPipelineCompiler(int threadCount, boolean autoRefresh)
    {
        this(() -> Executors.newFixedThreadPool(threadCount), autoRefresh);
    }

    /**
     *
     * @param provider
     * @param autoRefresh
     */
    public ParallelPipelineCompiler(ExecutorServiceProvider provider, boolean autoRefresh)
    {
        this.provider = provider;
        this.autoRefresh = autoRefresh;
    }

    @Override
    public ParallelPipelineRuntime compile(Collection<Node> inputNodes) throws CompilationException
    {
        List<Node> startNodes = new PipelineCompiler().compileNodes(inputNodes)
            .stream()
            .filter(n -> n.getRequirements().isEmpty())
            .collect(Collectors.toList())
        ;

        return new ParallelPipelineRuntime(
            startNodes,
            this.provider,
            this.autoRefresh
        );
    }
}
