package com.github.chrbayer84.trigrams;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class TrigramsMockAccumulo
{
    public static void main(String[] args) throws IOException,
            AccumuloException, AccumuloSecurityException, TableExistsException,
            TableNotFoundException
    {
        Instance mock = new MockInstance("test");
        Connector connector = mock.getConnector("root", "password");
        connector.tableOperations().create("TRIGRAMS");

        new IngestTextFile().readFromTextFile(connector, "TRIGRAMS",
                new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS)
                        .setMaxMemory(10000000).setMaxWriteThreads(5));

        Mutation m = new Mutation(new Text("john"));
        m.put("info:name", "", "john henry");
        m.put("info:gender", "", "male");
        m.put("friend:old", "mark", "");
        wr.addMutation(m);
        m = new Mutation(new Text("mary"));
        m.put("info:name", "", "mark wiggins");
        m.put("info:gender", "", "female");
        m.put("friend:new", "mark", "");
        m.put("friend:old", "lucas", "");
        m.put("friend:old", "aaron", "");
        wr.addMutation(m);
        wr.close();

        Scanner scanner = connector.createScanner("TABLEA",
                new Authorizations());
        scanner.setRange(new Range("a", "z"));
        scanner.fetchColumnFamily(new Text("friend:old"));
        Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
        while (iterator.hasNext())
        {
            Map.Entry<Key, Value> entry = iterator.next();
            Key key = entry.getKey();
            System.out.println("Old Friends: " + key.getRow() + " -> "
                    + key.getColumnQualifier());
        }
    }
}