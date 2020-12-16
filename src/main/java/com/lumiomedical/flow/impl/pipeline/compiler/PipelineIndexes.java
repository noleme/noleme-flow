package com.lumiomedical.flow.impl.pipeline.compiler;

import com.lumiomedical.flow.recipient.Recipient;

import java.util.Map;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/14
 */
public final class PipelineIndexes
{
    public final Map<String, Recipient> recipients;

    public PipelineIndexes(Map<String, Recipient> recipientsIndex)
    {
        this.recipients = recipientsIndex;
    }
}
