package com.lumiomedical.flow.compiler.pipeline.parallel;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.FlowCompiler;
import com.lumiomedical.flow.compiler.pipeline.PipelineCompiler;
import com.lumiomedical.flow.compiler.pipeline.parallel.executor.ExecutorServiceProvider;
import com.lumiomedical.flow.compiler.pipeline.parallel.executor.Executors;
import com.lumiomedical.flow.node.Node;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
public class ParallelCompiler implements FlowCompiler<ParallelRuntime>
{
    private final ExecutorServiceProvider provider;
    private boolean autoRefresh;

    /**
     *
     */
    public ParallelCompiler()
    {
        this(Runtime.getRuntime().availableProcessors(), true);
    }

    /**
     *
     */
    public ParallelCompiler(int threadCount, boolean autoRefresh)
    {
        this(() -> Executors.newFixedThreadPool(threadCount), autoRefresh);
    }

    /**
     *
     * @param provider
     * @param autoRefresh
     */
    public ParallelCompiler(ExecutorServiceProvider provider, boolean autoRefresh)
    {
        this.provider = provider;
        this.autoRefresh = autoRefresh;
    }

    @Override
    public ParallelRuntime compile(Collection<Node> inputNodes) throws CompilationException
    {
        List<Node> startNodes = new PipelineCompiler().compileNodes(inputNodes)
            .stream()
            .filter(n -> n.getRequirements().isEmpty())
            .collect(Collectors.toList())
        ;

        return new ParallelRuntime(
            startNodes,
            this.provider,
            this.autoRefresh
        );
    }
}
