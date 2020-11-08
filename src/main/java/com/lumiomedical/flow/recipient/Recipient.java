package com.lumiomedical.flow.recipient;

import com.lumiomedical.flow.Sink;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/10/19
 */
public final class Recipient <T> extends Sink<T>
{
    private final String name;

    /**
     * @param name
     */
    public Recipient(String name)
    {
        super(new RecipientContentLoader<>());
        this.name = name;
    }

    /**
     *
     * @return
     */
    public String getName()
    {
        return this.name;
    }

    /**
     *
     * @return
     */
    public T getContent()
    {
        return ((RecipientContentLoader<T>)this.getActor()).getContent();
    }
}
