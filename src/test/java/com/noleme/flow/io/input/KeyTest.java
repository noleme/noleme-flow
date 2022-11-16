package com.noleme.flow.io.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class KeyTest
{
    @Test
    public void testIndexing_randomUid()
    {
        Set<Key<?>> keys = new HashSet<>();
        keys.add(Input.key(String.class));
        keys.add(Input.key(String.class));
        keys.add(Input.key(Integer.class));

        Assertions.assertEquals(3, keys.size());
    }

    @Test
    public void testIndexing_named()
    {
        var key0 = Input.key("custom_name");
        var key1 = Input.key("custom_name");
        var key2 = Input.key("custom_name");

        Set<Key<?>> keys = new HashSet<>();
        keys.add(key0);
        keys.add(key1);
        keys.add(key2);

        /* We expect all add calls beyond add(key0) to be ineffective (as they should be effectively equal) */
        Assertions.assertEquals(1, keys.size());

        @SuppressWarnings("unchecked")
        var indexedKey = Assertions.assertDoesNotThrow(() -> keys.stream()
            .findFirst()
            .map(k -> (Key<String>) k)
            .orElseThrow()
        );

        Assertions.assertSame(key0, indexedKey);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testValidation()
    {
        Key<?> key0 = Input.key(String.class);

        Assertions.assertDoesNotThrow(() -> ((Key<Object>) key0).validate("123"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ((Key<Object>) key0).validate(123));

        Key<?> key1 = Input.key(Integer.class);

        Assertions.assertDoesNotThrow(() -> ((Key<Object>) key1).validate(123));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ((Key<Object>) key1).validate("123"));

        Key<Object> key2 = Input.key("custom_name");

        Assertions.assertDoesNotThrow(() -> key2.validate(123));
        Assertions.assertDoesNotThrow(() -> key2.validate("123"));
        Assertions.assertDoesNotThrow(() -> key2.validate(new ArbitraryObject()));
    }

    private static class ArbitraryObject {}
}
