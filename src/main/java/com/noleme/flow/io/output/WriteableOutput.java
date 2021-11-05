package com.noleme.flow.io.output;

import java.time.Instant;

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

    /**
     *
     * @param start
     * @return
     */
    WriteableOutput setStartTime(Instant start);

    /**
     * 
     * @param end
     * @return
     */
    WriteableOutput setEndTime(Instant end);
}
