package com.github.chrbayer84.trigrams;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class WeightCombiner extends Combiner
{
    @Override
    public void init(SortedKeyValueIterator<Key, Value> source,
            Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        options.put(ALL_OPTION, "true");
        super.init(source, options, env);
    }

    @Override
    public Value reduce(Key key, Iterator<Value> iter)
    {
        long count = 0;

        while (iter.hasNext())
        {
            String weightString = iter.next().toString();

            if (!weightString.isEmpty())
            {
                long val = Long.parseLong(weightString);
                count += val;
            }
        }
        return new Value(Long.toString(count).getBytes());
    }
}
