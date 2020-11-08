package com.lumiomedical.flow.compiler;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public interface FlowRuntime
{
    /**
     *
     * @throws RunException
     */
    void run() throws RunException;

    /**
     *
     * @param name
     * @param type
     * @param <T>
     * @return
     * @throws RunException
     */
    <T> T getSample(String name, Class<T> type) throws RunException;
}
