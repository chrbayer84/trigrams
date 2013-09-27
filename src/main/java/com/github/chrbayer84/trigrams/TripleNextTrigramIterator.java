package com.github.chrbayer84.trigrams;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.base.Throwables;

public class TripleNextTrigramIterator implements
        SortedKeyValueIterator<Key, Value>
{
    private SortedKeyValueIterator<Key, Value> source;

    private Key currentKey;

    private Value currentValue;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source,
            Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        this.source = source;
    }

    @Override
    public boolean hasTop()
    {
        return source.hasTop();
    }

    @Override
    public void next() throws IOException
    {
        long highestWeight = Long.MAX_VALUE;
        int count = 0;
        // maximum of 10 tries for to find the next value, this will ensure
        // that 10 keys with the same weight will be considered
        int maxRandomCount = TrigramsMockAccumulo.RANDOM.nextInt(10);
        while (source.hasTop())
        {
            currentKey = source.getTopKey();
            currentValue = source.getTopValue();
            long weight = Long.parseLong(currentValue.toString());
            // weightCombiner will have sorted keys by weight, so highest
            // weight will be up front. Only iterate over the next keys if
            // weight is equal for the next key.
            if (highestWeight < weight || maxRandomCount == count++)
            {
                break;
            }
            highestWeight = weight;
            source.next();
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies,
            boolean inclusive) throws IOException
    {
        source.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey()
    {
        if (currentKey == null)
        {
            try
            {
                next();
            }
            catch (IOException e)
            {
                Throwables.propagate(e);
            }
        }
        return currentKey;
    }

    @Override
    public Value getTopValue()
    {
        if (currentValue == null)
        {
            try
            {
                next();
            }
            catch (IOException e)
            {
                Throwables.propagate(e);
            }
        }
        return currentValue;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env)
    {
        return source.deepCopy(env);
    }
}
