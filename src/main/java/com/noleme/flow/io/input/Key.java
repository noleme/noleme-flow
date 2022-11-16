package com.noleme.flow.io.input;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class Key<T>
{
    private final Class<T> type;
    private final UUID uid;

    Key(Class<T> type)
    {
        this(type, UUID.randomUUID());
    }

    Key(Class<T> type, String name)
    {
        this(type, UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)));
    }

    private Key(Class<T> type, UUID uid)
    {
        if (type == null)
            throw new IllegalArgumentException("A Key cannot be initialized with a null type");
        if (uid == null)
            throw new IllegalArgumentException("A Key cannot be initialized with a null uid");

        this.type = type;
        this.uid = uid;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Key<?> key = (Key<?>) o;
        return this.uid.equals(key.uid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.uid);
    }

    T validate(T value)
    {
        if (!this.type.isInstance(value))
            throw new IllegalArgumentException("The provided value is not compatible with this key's type");
        return value;
    }
}
