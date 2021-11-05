package com.noleme.flow.annotation;

import jdk.jfr.Label;

import java.lang.annotation.*;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
@Documented
@Inherited
@Label("Experimental")
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
public @interface Experimental
{
    String value() default "";
}
