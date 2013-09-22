package com.github.chrbayer84.trigrams;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

public class WeightCombiner extends Combiner
{
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
