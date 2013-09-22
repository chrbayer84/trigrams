package com.github.chrbayer84.trigrams;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class TrigramsMockAccumulo
{
    public static final String KEY_SEPERATOR = "|";

    public static void main(String[] args) throws IOException,
            AccumuloException, AccumuloSecurityException, TableExistsException,
            TableNotFoundException
    {
        Instance mock = new MockInstance("test");
        Connector connector = mock.getConnector("root", "password");
        connector.tableOperations().create("TRIGRAMS");

        BatchWriter writer = connector.createBatchWriter("TRIGRAMS",
                new BatchWriterConfig()                        );
        // ingest data from file
        File file = new File( "src/main/resources/sawyer.txt");
        new TripleIngestTextFile().readFromTextFile(file, writer);
        writer.close();
        
        Scanner scanner = connector.createScanner("TABLEA",
                new Authorizations());
        scanner.setRange(new Range());
        scanner.addScanIterator( new IteratorSetting(15, "randomStartIterator", iteratorClass);
        
        
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