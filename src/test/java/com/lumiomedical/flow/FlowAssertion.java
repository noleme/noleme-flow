package com.lumiomedical.flow;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/10
 */
public class FlowAssertion
{
    private boolean activated = false;
    private int activationCount = 0;

    public boolean isActivated()
    {
        return activated;
    }

    public int getActivationCount()
    {
        return this.activationCount;
    }

    public FlowAssertion activate()
    {
        this.activated = true;
        this.activationCount += 1;
        return this;
    }
}
