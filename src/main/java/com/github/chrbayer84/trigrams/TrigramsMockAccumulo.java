package com.github.chrbayer84.trigrams;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

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
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TrigramsMockAccumulo
{
    public static final String KEY_SEPARATOR = " ";

    public static void main(String[] args) throws IOException,
            AccumuloException, AccumuloSecurityException, TableExistsException,
            TableNotFoundException
    {
        Instance mock = new MockInstance();
        Connector connector = mock.getConnector("root", new PasswordToken(
                new byte[0]));
        connector.tableOperations().create("TRIGRAMS");
        // connector.tableOperations()
        // .attachIterator(
        // "TRIGRAMS",
        // new IteratorSetting(15, "weightCombiner",
        // WeightCombiner.class));

        BatchWriter writer = connector.createBatchWriter("TRIGRAMS",
                new BatchWriterConfig());
        // ingest data from file
        File file = new File("src/main/resources/sawyer.txt");
        Files.readLines(file, Charsets.UTF_8,
                new StringLineProcessor(3, writer));
        writer.close();

        Scanner scanner = connector.createScanner("TRIGRAMS",
                new Authorizations());
        scanner.setRange(new Range());
        IteratorSetting iteratorSetting = new IteratorSetting(15,
                "nextTrigramIterator", TripleNextTrigramIterator.class);
        iteratorSetting.addOption("start", "THE ADVENTURES");
        iteratorSetting.addOption("maxWords", "1000");
        // scanner.clearScanIterators();
        scanner.addScanIterator(iteratorSetting);

        // scanner.fetchColumnFamily(new Text("friend:old"));
        Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
        while (iterator.hasNext())
        {
            Map.Entry<Key, Value> entry = iterator.next();
            Key key = entry.getKey();
            // System.out.println("Old Friends: " + key.getRow() + " -> "
            // + key.getColumnQualifier());
        }
    }
}