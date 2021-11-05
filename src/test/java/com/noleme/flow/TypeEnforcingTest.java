package com.noleme.flow;

import com.noleme.flow.impl.pipeline.stream.IterableGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class TypeEnforcingTest
{
    @Test
    void testLeadEnforcing_shouldAcceptLead()
    {
        Assertions.assertDoesNotThrow(() -> {
            Flow.from(() -> 123)
                .pipe(i -> i + 2)
                .asLead()
            ;
        });
    }

    @Test
    void testLeadEnforcing_shouldRejectStream()
    {
        Assertions.assertThrows(RuntimeException.class, () -> {
            Flow.from(() -> List.of(1, 2, 3))
                .stream(IterableGenerator::new)
                .pipe(i -> i + 2)
                .asLead()
            ;
        });
    }

    @Test
    void testStreamEnforcing_shouldAcceptStream()
    {
        Assertions.assertDoesNotThrow(() -> {
            Flow.from(() -> List.of(1, 2, 3))
                .stream(IterableGenerator::new)
                .pipe(i -> i + 2).asStream()
            ;
        });
    }

    @Test
    void testStreamEnforcing_shouldRejectLead()
    {
        Assertions.assertThrows(RuntimeException.class, () -> {
            Flow.from(() -> 123)
                .pipe(i -> i + 2)
                .asStream()
            ;
        });
    }
}
