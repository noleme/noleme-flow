package com.noleme.flow;

import com.noleme.flow.node.Node;

/**
 * Concept representing a Node with a potential upstream.
 * It features an "input" of type I.
 *
 * FlowIn nodes include Pipes and Sinks.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public interface FlowIn <I> extends Node
{

}
