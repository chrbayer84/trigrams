package com.github.chrbayer84.trigrams;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.LineProcessor;

class PairStringLineProcessor implements LineProcessor<String>
{
    private final BatchWriter writer;

    private final LinkedList<String> before = Lists.newLinkedList();

    private static final Pattern spacePattern = Pattern.compile("\\s");

    private final int N;

    PairStringLineProcessor(int n, BatchWriter writer)
    {
        N = n;
        this.writer = writer;
    }

    @Override
    public boolean processLine(String line) throws IOException
    {
        // get current line from
        String[] currentLine = spacePattern.split(line);
        // skip empty lines
        if ((currentLine.length == 0) || currentLine.length == 1
                && currentLine[0].isEmpty())
        {
            return true;
        }
        // get words wrapped from the last line
        // push words from the end of the last line to temp storage
        List<String> tmpList = Lists.newArrayList(before);
        Collections.addAll(tmpList, currentLine);
        String[] words = tmpList.toArray(new String[tmpList.size()]);
        before.clear();
        // process all words on that line
        for (int i = 0; i < words.length - N + 1; i++)
        {
            Text rowId = new Text(words[i]);
            Text columnFamily = new Text("triple");
            Text columnQualifier = new Text("qualifier");
            Mutation m = new Mutation(rowId);
            m.put(columnFamily, columnQualifier,
                    new Value(words[i + 1].getBytes()));
            try
            {
                writer.addMutation(m);
            }
            catch (MutationsRejectedException e)
            {
                Throwables.propagate(e);
            }
        }
        int start = (currentLine.length - N > 0) ? currentLine.length - N : 0;
        int end = (N >= currentLine.length) ? 0 : currentLine.length;
        // get words from wrapped line
        for (int i = start; i < end; i++)
        {
            before.push(currentLine[i]);
        }
        return true;
    }

    @Override
    public String getResult()
    {
        return null;
    }
}