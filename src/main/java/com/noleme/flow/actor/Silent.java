package com.noleme.flow.actor;

import java.lang.annotation.*;

/**
 * This annotation is expected to be used for "technical" nodes, no-op nodes that are used in specific graph-building situations.
 * At the time of this writing, the only impact that is to be expected is that the actor won't produce logs upon execution.
 *
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Silent
{

}
