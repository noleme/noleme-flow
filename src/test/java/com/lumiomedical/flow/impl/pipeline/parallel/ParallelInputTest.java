package com.lumiomedical.flow.impl.pipeline.parallel;

import com.lumiomedical.flow.Flow;
import com.lumiomedical.flow.FlowAssertion;
import com.lumiomedical.flow.actor.accumulator.AccumulationException;
import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.impl.parallel.ParallelCompiler;
import com.lumiomedical.flow.impl.pipeline.stream.IterableGenerator;
import com.lumiomedical.flow.io.input.Input;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class ParallelInputTest
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

        Flow.runAsParallel(Input.of("my_input", 3), flow);

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

        var output = Flow.runAsParallel(Input.of("my_input", new Integer[]{1, 3, 4}), flow);
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

        var output = Flow.runAsParallel(Input.of("my_input", List.of(234, 244)), flow);
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

        var runtime = new ParallelCompiler().compile(flow);

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
            .accumulate(vals -> vals.stream().reduce(Integer::sum).orElseThrow(() -> new AccumulationException("Could not sum values.")))
            .collect()
        ;

        var runtime = new ParallelCompiler().compile(flow);

        var output1 = runtime.run(Input.of("my_input", List.of(234, 244)));
        Assertions.assertEquals(482, output1.get(flow));

        var output2 = runtime.run(Input.of("my_input", List.of(234, 94, 12, 2, 244)));
        Assertions.assertEquals(596, output2.get(flow));

        var output3 = runtime.run(Input.of("my_input", List.of(234, 244, 2)));
        Assertions.assertEquals(486, output3.get(flow));
    }
}
