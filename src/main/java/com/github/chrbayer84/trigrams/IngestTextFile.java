package com.github.chrbayer84.trigrams;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.base.Charsets;
import com.google.common.io.ByteProcessor;
import com.google.common.io.Files;

public class IngestTextFile
{
    public void readFromTextFile(File file, Connector connector, String tableName,
            BatchWriterConfig batchWriterConfig) throws TableNotFoundException,
            MutationsRejectedException
    {
        BatchWriter writer = connector.createBatchWriter(tableName,
                batchWriterConfig);
Files.readBytes(file, new ByteProcessor<T>()
{
} )
        for (char c = 'a'; c <= 'c'; c++)
        {
            Text rowId = new Text(key);
            Text columnFamily = new Text(String.valueOf(c));
            Text columnQualifier = c == 'a' ? new Text("start") : new Text(
                    "node");

            Mutation m = new Mutation(rowId);
            m.put(columnFamily, columnQualifier, new Value(value));
            writer.addMutation(m);
        }
    }
}
