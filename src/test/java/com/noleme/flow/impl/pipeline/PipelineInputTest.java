package com.noleme.flow.impl.pipeline;

import com.noleme.flow.Flow;
import com.noleme.flow.FlowAssertion;
import com.noleme.flow.actor.accumulator.AccumulationException;
import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.pipeline.stream.IterableGenerator;
import com.noleme.flow.io.input.Input;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class PipelineInputTest
{
    @Test
    void testInputInteger() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .<Integer>from("my_input")
            .pipe(i -> i + 2)
            .sink(i -> {
                assertion.activate();
                Assertions.assertEquals(5, i);
            })
        ;

        Flow.runAsPipeline(Input.of("my_input", 3), flow);

        Assertions.assertTrue(assertion.isActivated());
    }

    @Test
    void testInputIntegerTyped() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var input = Input.key(Integer.class);
        var flow = Flow
            .from(input)
            .pipe(i -> i + 2)
            .sink(i -> {
                assertion.activate();
                Assertions.assertEquals(5, i);
            })
        ;

        Flow.runAsPipeline(Input.of(input, 3), flow);

        Assertions.assertTrue(assertion.isActivated());
    }

    @Test
    void testInputArray() throws RunException, CompilationException
    {
        var flow = Flow
            .<Integer[]>from("my_input")
            .pipe(a -> a.length)
            .collect()
        ;

        var output = Flow.runAsPipeline(Input.of("my_input", new Integer[]{1, 3, 4}), flow);
        Assertions.assertEquals(3, output.get(flow));
    }

    @Test
    void testInputGeneric() throws RunException, CompilationException
    {
        var flow = Flow
            .<List<Integer>>from("my_input")
            .pipe(List::size)
            .collect()
        ;

        var output = Flow.runAsPipeline(Input.of("my_input", List.of(234, 244)), flow);
        Assertions.assertEquals(2, output.get(flow));
    }

    @Test
    void testInputRepeat() throws RunException, CompilationException
    {
        var flow = Flow
            .<List<Integer>>from("my_input")
            .pipe(List::size)
            .collect()
        ;

        var runtime = new PipelineCompiler().compile(flow);

        var output1 = runtime.run(Input.of("my_input", List.of(234, 244)));
        Assertions.assertEquals(2, output1.get(flow));

        var output2 = runtime.run(Input.of("my_input", List.of(234, 94, 12, 2, 244)));
        Assertions.assertEquals(5, output2.get(flow));

        var output3 = runtime.run(Input.of("my_input", List.of(234, 244, 2)));
        Assertions.assertEquals(3, output3.get(flow));
    }

    @Test
    void testInputStreamRepeat() throws RunException, CompilationException
    {
        var flow = Flow
            .<List<Integer>>from("my_input")
            .stream(IterableGenerator::new)
            .pipe(i -> i + 2)
            .accumulate(vals -> vals.stream().reduce(Integer::sum).orElseThrow(() -> new AccumulationException("Could not sum values."))).asFlow()
            .collect()
        ;

        var runtime = new PipelineCompiler().compile(flow);

        var output1 = runtime.run(Input.of("my_input", List.of(234, 244)));
        Assertions.assertEquals(482, output1.get(flow));

        var output2 = runtime.run(Input.of("my_input", List.of(234, 94, 12, 2, 244)));
        Assertions.assertEquals(596, output2.get(flow));

        var output3 = runtime.run(Input.of("my_input", List.of(234, 244, 2)));
        Assertions.assertEquals(486, output3.get(flow));
    }

    private interface A {}
    private static class B implements A {}
    private static class C implements A {}
}
