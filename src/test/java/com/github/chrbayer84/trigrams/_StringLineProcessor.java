package com.github.chrbayer84.trigrams;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class _StringLineProcessor
{
    @Mock
    BatchWriter batchWriter;

    Map<String, String> result;

    @Before
    public void setup() throws MutationsRejectedException
    {
        result = Maps.newHashMap();
        MockitoAnnotations.initMocks(this);
        doAnswer(new Answer<Void>()
        {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable
            {
                Object[] args = invocation.getArguments();
                for (Object o : args)
                {
                    String key = new String(((Mutation) o).getRow());
                    String value = new String(((Mutation) o).getUpdates()
                            .get(0).getValue());
                    result.put(key, value);
                    System.out.println("key: " + key + " value: " + value);

                }
                return null;
            }

        }).when(batchWriter).addMutation(any(Mutation.class));
    }

    @Test
    @Ignore
    public void readFromTextFile() throws TableNotFoundException, IOException,
            MutationsRejectedException
    {
        File file = new File("src/main/resources/sawyer.txt");
        Files.readLines(file, Charsets.UTF_8, new StringLineProcessor(3,
                batchWriter));
    }

    @Test
    public void stringLineProcessor() throws MutationsRejectedException,
            TableNotFoundException, IOException
    {
        String probe = "Tom blanched and dropped his eyes.";
        StringLineProcessor stringLineProcessor = new StringLineProcessor(3,
                batchWriter);
        stringLineProcessor.processLine(probe);
        Assert.assertEquals(4, result.size());
        Assert.assertEquals("1", result.get("Tom blanched"));
        Assert.assertEquals("1", result.get("blanched and"));
        Assert.assertEquals("1", result.get("and dropped"));
        Assert.assertEquals("1", result.get("dropped his"));
    }

    @Test
    public void multiline_stringLineProcessor()
            throws MutationsRejectedException, TableNotFoundException,
            IOException
    {
        String probe1 = "Tom blanched and dropped his eyes.";
        String probe2 = "It's a bad sign.";
        StringLineProcessor stringLineProcessor = new StringLineProcessor(3,
                batchWriter);
        stringLineProcessor.processLine(probe1);
        stringLineProcessor.processLine(probe2);
        Assert.assertEquals(8, result.size());
        Assert.assertEquals("1", result.get("Tom blanched"));
        Assert.assertEquals("1", result.get("blanched and"));
        Assert.assertEquals("1", result.get("and dropped"));
        Assert.assertEquals("1", result.get("dropped his"));
        Assert.assertEquals("1", result.get("his eyes."));
        Assert.assertEquals("1", result.get("eyes. It%27s"));
        Assert.assertEquals("1", result.get("It%27s a"));
        Assert.assertEquals("1", result.get("a bad"));
    }

    @Test
    public void empty_stringLineProcessor() throws MutationsRejectedException,
            TableNotFoundException, IOException
    {
        String probe = "";
        StringLineProcessor stringLineProcessor = new StringLineProcessor(3,
                batchWriter);
        stringLineProcessor.processLine(probe);
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void short_stringLineProcessor() throws MutationsRejectedException,
            TableNotFoundException, IOException
    {
        String probe = "Tom blanched and";
        StringLineProcessor stringLineProcessor = new StringLineProcessor(3,
                batchWriter);
        stringLineProcessor.processLine(probe);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("1", result.get("Tom blanched"));
    }

    @Test
    public void tooShort_stringLineProcessor()
            throws MutationsRejectedException, TableNotFoundException,
            IOException
    {
        String probe = "Tom blanched";
        StringLineProcessor stringLineProcessor = new StringLineProcessor(3,
                batchWriter);
        stringLineProcessor.processLine(probe);
        Assert.assertEquals(0, result.size());
    }
}
