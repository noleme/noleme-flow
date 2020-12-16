package com.lumiomedical.flow.recipient;

import com.lumiomedical.flow.actor.loader.Loader;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/10/19
 */
final class RecipientContentLoader<T> implements Loader<T>
{
    private T content;
    private boolean filled;

    public RecipientContentLoader()
    {
        this.filled = false;
    }

    @Override
    public void load(T input)
    {
        this.content = input;
        this.filled = true;
    }

    public T getContent()
    {
        return this.content;
    }

    public boolean isFilled()
    {
        return this.filled;
    }
}
