package com.github.chrbayer84.trigrams;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.base.Throwables;

public class TripleNextTrigramIterator implements
        SortedKeyValueIterator<Key, Value>
{
    private static final Pattern KEY_SEPERATOR_PATTERN = Pattern.compile(
    // "\\"+
            TrigramsMockAccumulo.KEY_SEPARATOR);

    private SortedKeyValueIterator<Key, Value> source;

    private Key currentKey;

    private Value currentValue;

    private Key nextKey;

    private boolean firstRun = true;

    private int MAXWORDS;

    private int valuesGenerated;

    private boolean random;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source,
            Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        this.source = source;
        // get starting value from options and set currentKey to it, next() will
        // start off this key
        nextKey = new Key(options.get("start"));
        // get max word count
        MAXWORDS = Integer.parseInt(options.get("maxWords"));
    }

    @Override
    public boolean hasTop()
    {
        return currentKey != null || nextKey != null;
    }

    @Override
    public void next() throws IOException
    {
        // handle first run, print key components that will start the story
        if (firstRun)
        {
            String[] keyComponents = KEY_SEPERATOR_PATTERN.split(nextKey
                    .getRow().toString());
            System.out.print(keyComponents[0] + " " + keyComponents[1] + " ");
            firstRun = false;
        }
        Range range = random ? IteratorUtil
                .maximizeStartKeyTimeStamp(new Range(nextKey, null))
                : IteratorUtil
                        .minimizeEndKeyTimeStamp(new Range(nextKey, null));
        random = !random;
        source.seek(range, new ArrayList<ByteSequence>(), false);

        if (source.hasTop() && valuesGenerated < MAXWORDS)
        {
            currentKey = source.getTopKey();
            // assert we got the same key
            // Preconditions.checkState(nextKey.equals(currentKey.getRow()
            // .toString()));
            // get value
            currentValue = source.getTopValue();
            String value = new String(currentValue.get());
            // construct next key
            String[] keyComponents = KEY_SEPERATOR_PATTERN.split(currentKey
                    .toString());
            nextKey = new Key(keyComponents[1]
                    + TrigramsMockAccumulo.KEY_SEPARATOR + value);
            // print value to console
            System.out.print(URLDecoder.decode(value, "UTF-8") + " ");
            valuesGenerated++;
        }
        else
        {
            currentKey = null;
            currentValue = null;
            nextKey = null;
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
