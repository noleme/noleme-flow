package com.lumiomedical.flow.io.output;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public interface WriteableOutput extends Output
{
    /**
     *
     * @param identifier
     * @param value
     */
    WriteableOutput set(String identifier, Object value);
}
