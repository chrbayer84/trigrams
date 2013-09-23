package com.github.chrbayer84.trigrams;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Collections;
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

class StringLineProcessor implements LineProcessor<String>
{
    private final BatchWriter writer;

    private final List<String> before = Lists.newArrayList();

    private static final Pattern spacePattern = Pattern.compile(" ");

    private final int N;

    StringLineProcessor(int n, BatchWriter writer)
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
        // re-add newline to handle paragraphs
        currentLine[currentLine.length - 1] += "\n";
        // get words wrapped from the last line
        // push words from the end of the last line to temp storage
        List<String> tmpList = Lists.newArrayList(before);
        Collections.addAll(tmpList, currentLine);
        String[] words = tmpList.toArray(new String[tmpList.size()]);
        before.clear();
        // process all words on that line
        for (int i = 0; i < words.length - N + 1; i++)
        {
            // key length: N
            StringBuilder rowIdBuilder = new StringBuilder();
            for (int keyIndex = 0; keyIndex < N; keyIndex++)
            {
                rowIdBuilder.append(URLEncoder.encode(words[i + keyIndex],
                        "UTF-8"));
                // don't add key separator at the last component
                if (keyIndex != N - 1)
                {
                    rowIdBuilder.append(TrigramsMockAccumulo.KEY_SEPARATOR);
                }
            }
            Text rowId = new Text(rowIdBuilder.toString());
            Text columnFamily = new Text("weight");
            Text columnQualifier = new Text("qualifier");
            Mutation m = new Mutation(rowId);
            // put count == 1 as value
            m.put(columnFamily, columnQualifier, new Value("1".getBytes()));
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
            before.add(currentLine[i]);
        }
        return true;
    }

    @Override
    public String getResult()
    {
        return null;
    }
}