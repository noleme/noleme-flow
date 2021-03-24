package com.noleme.flow;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/10
 */
public class FlowState <T>
{
    private T value;

    public T getValue()
    {
        return this.value;
    }

    public FlowState<T> setValue(T value)
    {
        this.value = value;
        return this;
    }
}
