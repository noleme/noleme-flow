package com.lumiomedical.flow;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/10
 */
public class FlowAssertion
{
    private boolean activated = false;

    public boolean isActivated()
    {
        return activated;
    }

    public FlowAssertion setActivated(boolean activated)
    {
        this.activated = activated;
        return this;
    }
}
