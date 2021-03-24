package com.noleme.flow;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/10
 */
public final class FlowDealer
{
    private FlowDealer()
    {
    }

    public static Pipe<Integer, Integer> sourceReturns8()
    {
        return Flow.from(() -> 3)
            .into((i) -> i + 1)
            .into((i) -> i * 2)
        ;
    }

    public static Pipe<Integer, Integer> sourceReturns9()
    {
        return Flow.from(() -> 2)
            .into((i) -> i + 1)
            .into((i) -> i * 3);
    }

    public static Pipe<Integer, Integer> sourceReturns6()
    {
        return Flow.from(() -> 4)
            .into((i) -> i + 2);
    }

    public static Pipe<Integer, Integer> transformerAdds(FlowOut<Integer> input, int add)
    {
        return input.into((i) -> i + add);
    }

    public static Pipe<Integer, Integer> transformerSubstracts4(FlowOut<Integer> input, int sub)
    {
        return input.into((i) -> i - sub);
    }

    public static Pipe<Integer, Integer> transformerMultipliesBy5(FlowOut<Integer> input, int mult)
    {
        return input.into((i) -> i * mult);
    }

    public static Join<Integer, Integer, Integer> joinSub(FlowOut<Integer> a, FlowOut<Integer> b)
    {
        return Flow.join(a, b, (i1, i2) -> i1 - i2);
    }

    public static Join<Integer, Integer, Integer> joinMult(FlowOut<Integer> a, FlowOut<Integer> b)
    {
        return Flow.join(a, b, (i1, i2) -> i1 * i2);
    }

    public static Join<Integer, Integer, Integer> joinPow(FlowOut<Integer> a, FlowOut<Integer> b)
    {
        return Flow.join(a, b, (i1, i2) -> i1 ^ i2);
    }
}
