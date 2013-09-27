package com.github.chrbayer84.trigrams;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

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

    private static final Pattern KEY_SEPERATOR_PATTERN = Pattern
            .compile(TrigramsMockAccumulo.KEY_SEPARATOR);

    private static final Pattern PUNCTUATION_PATTERN = Pattern
            .compile("[.\\?\\!]");

    static final Random RANDOM = new Random(System.currentTimeMillis());

    private static final int N = 3;

    public static void main(String[] args) throws IOException,
            AccumuloException, AccumuloSecurityException, TableExistsException,
            TableNotFoundException
    {
        Instance mock = new MockInstance();
        Connector connector = mock.getConnector("root", new PasswordToken(
                new byte[0]));
        connector.tableOperations().create("TRIGRAMS");
        // set up combiner
        IteratorSetting combinerSetting = new IteratorSetting(5,
                "weightCombiner", WeightCombiner.class);
        combinerSetting.addOption("all", "true");
        connector.tableOperations().attachIterator("TRIGRAMS", combinerSetting);
        BatchWriter writer = connector.createBatchWriter("TRIGRAMS",
                new BatchWriterConfig());
        // ingest data from file
        File file = new File("src/main/resources/sawyer.txt");
        Files.readLines(file, Charsets.UTF_8,
                new StringLineProcessor(N, writer));
        writer.close();

        Scanner weightScanner = connector.createScanner("TRIGRAMS",
                new Authorizations());
        Iterator<Map.Entry<Key, Value>> weightIterator = weightScanner
                .iterator();
        while (weightIterator.hasNext())
        {
            weightIterator.next();
        }

        Key nextKey = new Key("The boy");
        System.out.print(nextKey.getRow() + " ");
        int sentencesCount = 0;
        for (int maxNumberOfWords = 0; maxNumberOfWords < 1000; maxNumberOfWords++)
        {
            Scanner scanner = connector.createScanner("TRIGRAMS",
                    new Authorizations());
            // why does that not work?
            Key endKey = new Key(Range.followingPrefix(nextKey.getRow()));
            scanner.setRange(new Range(nextKey, null));

            IteratorSetting iteratorSetting = new IteratorSetting(15,
                    "nextTrigramIterator", TripleNextTrigramIterator.class);
            scanner.clearScanIterators();
            scanner.addScanIterator(iteratorSetting);
            Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

            Key currentKey = null;
            if (iterator.hasNext())
            {
                Map.Entry<Key, Value> entry = iterator.next();
                currentKey = entry.getKey();
            }
            scanner.close();
            if (currentKey == null)
            {
                // could not find any keys with the specified trigram
                return;
            }
            String[] keyComponents = KEY_SEPERATOR_PATTERN.split(currentKey
                    .getRow().toString());
            StringBuilder nextKeyBuilder = new StringBuilder();
            for (int keyIndex = 1; keyIndex < N - 1; keyIndex++)
            {
                nextKeyBuilder.append(keyComponents[keyIndex]);
                nextKeyBuilder.append(TrigramsMockAccumulo.KEY_SEPARATOR);
            }
            nextKey = new Key(nextKeyBuilder.toString()
                    + currentKey.getColumnFamily());
            // print value to console
            String nextWord = URLDecoder.decode(currentKey.getColumnFamily()
                    .toString(), "UTF-8");
            boolean endofSentence = PUNCTUATION_PATTERN.matcher(nextWord)
                    .find();
            System.out.print(nextWord);
            if (endofSentence)
            {
                if (sentencesCount > RANDOM.nextInt(3))
                {
                    System.out.print("\n");
                    sentencesCount = 0;
                }
                else
                {
                    System.out.print(" ");
                }
                sentencesCount++;
            }
            else
            {
                System.out.print(" ");
            }
        }
    }
}