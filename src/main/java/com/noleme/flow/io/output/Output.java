package com.noleme.flow.io.output;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public interface Output
{
    /**
     *
     * @param recipient
     * @param <T>
     * @return
     */
    <T> boolean has(Recipient<T> recipient);

    /**
     *
     * @param identifier
     * @return
     */
    boolean has(String identifier);

    /**
     *
     * @param recipient
     * @param <T>
     * @return
     */
    <T> T get(Recipient<T> recipient);

    /**
     *
     * @param identifier
     * @param type
     * @param <T>
     * @return
     */
    <T> T get(String identifier, Class<T> type);

    /**
     *
     * @param identifier
     * @return
     */
    Object get(String identifier);

    /**
     *
     * @return
     */
    Instant startTime();

    /**
     *
     * @return
     */
    Instant endTime();

    default Duration elapsedTime()
    {
        return Duration.between(this.startTime(), this.endTime());
    }

    default String elapsedTimeString()
    {
        var duration = this.elapsedTime();
        List<String> parts = new ArrayList<>();

        long days = duration.toDaysPart();
        if (days > 0)
            parts.add(days + "d");

        int hours = duration.toHoursPart();
        if (hours > 0 || !parts.isEmpty())
            parts.add(hours + "h");

        int minutes = duration.toMinutesPart();
        if (minutes > 0 || !parts.isEmpty())
            parts.add(minutes + "min");

        int seconds = duration.toSecondsPart();
        if (seconds > 0 || !parts.isEmpty())
            parts.add(seconds + "s");

        int millis = duration.toMillisPart();
        parts.add(millis + "ms");

        return String.join(", ", parts);
    }
}
