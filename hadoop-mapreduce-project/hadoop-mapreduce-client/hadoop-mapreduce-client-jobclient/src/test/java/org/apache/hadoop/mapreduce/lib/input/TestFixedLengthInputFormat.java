/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

public class TestFixedLengthInputFormat {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFixedLengthInputFormat.class);

  private static Configuration defaultConf;
  private static FileSystem localFs;
  private static Path workDir;

  // some chars for the record data
  private static char[] chars;
  private static Random charRand;

  @BeforeAll
  public static void onlyOnce() {
    try {
      defaultConf = new Configuration();
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
      // our set of chars
      chars = ("abcdefghijklmnopqrstuvABCDEFGHIJKLMN OPQRSTUVWXYZ1234567890)"
          + "(*&^%$#@!-=><?:\"{}][';/.,']").toCharArray();
      workDir = 
          new Path(new Path(System.getProperty("test.build.data", "."), "data"),
          "TestKeyValueFixedLengthInputFormat");
      charRand = new Random();
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  /**
   * 20 random tests of various record, file, and split sizes.  All tests have
   * uncompressed file as input.
   */
  @Test
  @Timeout(value = 500)
  public void testFormat() throws Exception {
    runRandomTests(null);
  }

  /**
   * 20 random tests of various record, file, and split sizes.  All tests have
   * compressed file as input.
   */
  @Test
  @Timeout(value = 500)
  public void testFormatCompressedIn() throws Exception {
    runRandomTests(new GzipCodec());
  }

  /**
   * Test with no record length set.
   */
  @Test
  @Timeout(value = 5)
  public void testNoRecordLength() throws Exception {
    localFs.delete(workDir, true);
    Path file = new Path(workDir, "testFormat.txt");
    createFile(file, null, 10, 10);
    // Create the job and do not set fixed record length
    Job job = Job.getInstance(defaultConf);
    FileInputFormat.setInputPaths(job, workDir);
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    boolean exceptionThrown = false;
    for (InputSplit split : splits) {
      try {
        TaskAttemptContext context = MapReduceTestUtil.
            createDummyMapTaskAttemptContext(job.getConfiguration());
        RecordReader<LongWritable, BytesWritable> reader =
            format.createRecordReader(split, context);
        MapContext<LongWritable, BytesWritable, LongWritable, BytesWritable>
            mcontext =
            new MapContextImpl<LongWritable, BytesWritable, LongWritable,
            BytesWritable>(job.getConfiguration(), context.getTaskAttemptID(),
            reader, null, null, MapReduceTestUtil.createDummyReporter(), split);
        reader.initialize(split, mcontext);
      } catch(IOException ioe) {
        exceptionThrown = true;
        LOG.info("Exception message:" + ioe.getMessage());
      }
    }
    assertTrue(exceptionThrown, "Exception for not setting record length:");
  }

  /**
   * Test with record length set to 0
   */
  @Test
  @Timeout(value = 5)
  public void testZeroRecordLength() throws Exception {
    localFs.delete(workDir, true);
    Path file = new Path(workDir, "testFormat.txt");
    createFile(file, null, 10, 10);
    Job job = Job.getInstance(defaultConf);
    // Set the fixed length record length config property 
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    format.setRecordLength(job.getConfiguration(), 0);
    FileInputFormat.setInputPaths(job, workDir);
    List<InputSplit> splits = format.getSplits(job);
    boolean exceptionThrown = false;
    for (InputSplit split : splits) {
      try {
        TaskAttemptContext context =
            MapReduceTestUtil.createDummyMapTaskAttemptContext(
            job.getConfiguration());
        RecordReader<LongWritable, BytesWritable> reader = 
            format.createRecordReader(split, context);
        MapContext<LongWritable, BytesWritable, LongWritable, BytesWritable>
            mcontext =
            new MapContextImpl<LongWritable, BytesWritable, LongWritable,
            BytesWritable>(job.getConfiguration(), context.getTaskAttemptID(),
            reader, null, null, MapReduceTestUtil.createDummyReporter(), split);
        reader.initialize(split, mcontext);
      } catch(IOException ioe) {
        exceptionThrown = true;
        LOG.info("Exception message:" + ioe.getMessage());
      }
    }
    assertTrue(exceptionThrown, "Exception for zero record length:");
  }

  /**
   * Test with record length set to a negative value
   */
  @Test
  @Timeout(value = 5)
  public void testNegativeRecordLength() throws Exception {
    localFs.delete(workDir, true);
    Path file = new Path(workDir, "testFormat.txt");
    createFile(file, null, 10, 10);
    // Set the fixed length record length config property 
    Job job = Job.getInstance(defaultConf);
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    format.setRecordLength(job.getConfiguration(), -10);
    FileInputFormat.setInputPaths(job, workDir);
    List<InputSplit> splits = format.getSplits(job);
    boolean exceptionThrown = false;
    for (InputSplit split : splits) {
      try {
        TaskAttemptContext context = MapReduceTestUtil.
            createDummyMapTaskAttemptContext(job.getConfiguration());
        RecordReader<LongWritable, BytesWritable> reader = 
            format.createRecordReader(split, context);
        MapContext<LongWritable, BytesWritable, LongWritable, BytesWritable>
            mcontext =
            new MapContextImpl<LongWritable, BytesWritable, LongWritable,
            BytesWritable>(job.getConfiguration(), context.getTaskAttemptID(),
            reader, null, null, MapReduceTestUtil.createDummyReporter(), split);
        reader.initialize(split, mcontext);
      } catch(IOException ioe) {
        exceptionThrown = true;
        LOG.info("Exception message:" + ioe.getMessage());
      }
    }
    assertTrue(exceptionThrown, "Exception for negative record length:");
  }

  /**
   * Test with partial record at the end of a compressed input file.
   */
  @Test
  @Timeout(value = 5)
  public void testPartialRecordCompressedIn() throws Exception {
    CompressionCodec gzip = new GzipCodec();
    runPartialRecordTest(gzip);
  }

  /**
   * Test with partial record at the end of an uncompressed input file.
   */
  @Test
  @Timeout(value = 5)
  public void testPartialRecordUncompressedIn() throws Exception {
    runPartialRecordTest(null);
  }

  /**
   * Test using the gzip codec with two input files.
   */
  @Test
  @Timeout(value = 5)
  public void testGzipWithTwoInputs() throws Exception {
    CompressionCodec gzip = new GzipCodec();
    localFs.delete(workDir, true);
    Job job = Job.getInstance(defaultConf);
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    format.setRecordLength(job.getConfiguration(), 5);
    ReflectionUtils.setConf(gzip, job.getConfiguration());
    FileInputFormat.setInputPaths(job, workDir);
    // Create files with fixed length records with 5 byte long records.
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, 
        "one  two  threefour five six  seveneightnine ten  ");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
        "ten  nine eightsevensix  five four threetwo  one  ");
    List<InputSplit> splits = format.getSplits(job);
    assertEquals(2, splits.size(), "compressed splits == 2");
    FileSplit tmp = (FileSplit) splits.get(0);
    if (tmp.getPath().getName().equals("part2.txt.gz")) {
      splits.set(0, splits.get(1));
      splits.set(1, tmp);
    }
    List<String> results = readSplit(format, splits.get(0), job);
    assertEquals(10, results.size(), "splits[0] length");
    assertEquals("splits[0][5]", "six  ", results.get(5));
    results = readSplit(format, splits.get(1), job);
    assertEquals(10, results.size(), "splits[1] length");
    assertEquals("splits[1][0]", "ten  ", results.get(0));
    assertEquals("splits[1][1]", "nine ", results.get(1));
  }

  // Create a file containing fixed length records with random data
  private ArrayList<String> createFile(Path targetFile, CompressionCodec codec,
                                       int recordLen,
                                       int numRecords) throws IOException {
    ArrayList<String> recordList = new ArrayList<String>(numRecords);
    OutputStream ostream = localFs.create(targetFile);
    if (codec != null) {
      ostream = codec.createOutputStream(ostream);
    }
    Writer writer = new OutputStreamWriter(ostream);
    try {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numRecords; i++) {
        for (int j = 0; j < recordLen; j++) {
          sb.append(chars[charRand.nextInt(chars.length)]);
        }
        String recordData = sb.toString();
        recordList.add(recordData);
        writer.write(recordData);
        sb.setLength(0);
      }
    } finally {
      writer.close();
    }
    return recordList;
  }

  private void runRandomTests(CompressionCodec codec) throws Exception {
    StringBuilder fileName = new StringBuilder("testFormat.txt");
    if (codec != null) {
      fileName.append(".gz");
    }
    localFs.delete(workDir, true);
    Path file = new Path(workDir, fileName.toString());
    int seed = new Random().nextInt();
    LOG.info("Seed = " + seed);
    Random random = new Random(seed);
    int MAX_TESTS = 20;
    LongWritable key;
    BytesWritable value;

    for (int i = 0; i < MAX_TESTS; i++) {
      LOG.info("----------------------------------------------------------");
      // Maximum total records of 999
      int totalRecords = random.nextInt(999)+1;
      // Test an empty file
      if (i == 8) {
         totalRecords = 0;
      }
      // Maximum bytes in a record of 100K
      int recordLength = random.nextInt(1024*100)+1;
      // For the 11th test, force a record length of 1
      if (i == 10) {
        recordLength = 1;
      }
      // The total bytes in the test file
      int fileSize = (totalRecords * recordLength);
      LOG.info("totalRecords=" + totalRecords + " recordLength="
          + recordLength);
      // Create the job 
      Job job = Job.getInstance(defaultConf);
      if (codec != null) {
        ReflectionUtils.setConf(codec, job.getConfiguration());
      }
      // Create the test file
      ArrayList<String> recordList =
          createFile(file, codec, recordLength, totalRecords);
      assertTrue(localFs.exists(file));
      //set the fixed length record length config property for the job
      FixedLengthInputFormat.setRecordLength(job.getConfiguration(),
          recordLength);

      int numSplits = 1;
      // Arbitrarily set number of splits.
      if (i > 0) {
        if (i == (MAX_TESTS-1)) {
          // Test a split size that is less than record len
          numSplits = (int)(fileSize/Math.floor(recordLength/2));
        } else {
          if (MAX_TESTS % i == 0) {
            // Let us create a split size that is forced to be 
            // smaller than the end file itself, (ensures 1+ splits)
            numSplits = fileSize/(fileSize - random.nextInt(fileSize));
          } else {
            // Just pick a random split size with no upper bound 
            numSplits = Math.max(1, fileSize/random.nextInt(Integer.MAX_VALUE));
          }
        }
        LOG.info("Number of splits set to: " + numSplits);
      }
      job.getConfiguration().setLong(
          "mapreduce.input.fileinputformat.split.maxsize", 
          (long)(fileSize/numSplits));

      // setup the input path
      FileInputFormat.setInputPaths(job, workDir);
      // Try splitting the file in a variety of sizes
      FixedLengthInputFormat format = new FixedLengthInputFormat();
      List<InputSplit> splits = format.getSplits(job);
      LOG.info("Actual number of splits = " + splits.size());
      // Test combined split lengths = total file size
      long recordOffset = 0;
      int recordNumber = 0;
      for (InputSplit split : splits) {
        TaskAttemptContext context = MapReduceTestUtil.
            createDummyMapTaskAttemptContext(job.getConfiguration());
        RecordReader<LongWritable, BytesWritable> reader = 
            format.createRecordReader(split, context);
        MapContext<LongWritable, BytesWritable, LongWritable, BytesWritable>
            mcontext =
            new MapContextImpl<LongWritable, BytesWritable, LongWritable,
            BytesWritable>(job.getConfiguration(), context.getTaskAttemptID(),
            reader, null, null, MapReduceTestUtil.createDummyReporter(), split);
        reader.initialize(split, mcontext);
        Class<?> clazz = reader.getClass();
        assertEquals(
            FixedLengthRecordReader.class, clazz, "RecordReader class should be FixedLengthRecordReader:");
        // Plow through the records in this split
        while (reader.nextKeyValue()) {
          key = reader.getCurrentKey();
          value = reader.getCurrentValue();
          assertEquals((long)(recordNumber*recordLength)
,               key.get(), "Checking key");
          String valueString = new String(value.getBytes(), 0,
              value.getLength());
          assertEquals(recordLength
,               value.getLength(), "Checking record length:");
          assertTrue(
             recordNumber < totalRecords, "Checking for more records than expected:");
          String origRecord = recordList.get(recordNumber);
          assertEquals(origRecord, valueString, "Checking record content:");
          recordNumber++;
        }
        reader.close();
      }
      assertEquals(
         recordList.size(), recordNumber, "Total original records should be total read records:");
    }
  }

  private static void writeFile(FileSystem fs, Path name, 
                                CompressionCodec codec,
                                String contents) throws IOException {
    OutputStream stm;
    if (codec == null) {
      stm = fs.create(name);
    } else {
      stm = codec.createOutputStream(fs.create(name));
    }
    stm.write(contents.getBytes());
    stm.close();
  }

  private static List<String> readSplit(FixedLengthInputFormat format, 
                                        InputSplit split, 
                                        Job job) throws Exception {
    List<String> result = new ArrayList<String>();
    TaskAttemptContext context = MapReduceTestUtil.
        createDummyMapTaskAttemptContext(job.getConfiguration());
    RecordReader<LongWritable, BytesWritable> reader =
        format.createRecordReader(split, context);
    MapContext<LongWritable, BytesWritable, LongWritable, BytesWritable>
        mcontext =
        new MapContextImpl<LongWritable, BytesWritable, LongWritable,
        BytesWritable>(job.getConfiguration(), context.getTaskAttemptID(),
        reader, null, null, MapReduceTestUtil.createDummyReporter(), split);
    LongWritable key;
    BytesWritable value;
    try {
      reader.initialize(split, mcontext);
      while (reader.nextKeyValue()) {
        key = reader.getCurrentKey();
        value = reader.getCurrentValue();
        result.add(new String(value.getBytes(), 0, value.getLength()));
      }
    } finally {
      reader.close();
    }
    return result;
  }

  private void runPartialRecordTest(CompressionCodec codec) throws Exception {
    localFs.delete(workDir, true);
    Job job = Job.getInstance(defaultConf);
    // Create a file with fixed length records with 5 byte long
    // records with a partial record at the end.
    StringBuilder fileName = new StringBuilder("testFormat.txt");
    if (codec != null) {
      fileName.append(".gz");
      ReflectionUtils.setConf(codec, job.getConfiguration());
    }
    writeFile(localFs, new Path(workDir, fileName.toString()), codec,
        "one  two  threefour five six  seveneightnine ten");
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    format.setRecordLength(job.getConfiguration(), 5);
    FileInputFormat.setInputPaths(job, workDir);
    List<InputSplit> splits = format.getSplits(job);
    if (codec != null) {
      assertEquals(1, splits.size(), "compressed splits == 1");
    }
    boolean exceptionThrown = false;
    for (InputSplit split : splits) {
      try {
        List<String> results = readSplit(format, split, job);
      } catch(IOException ioe) {
        exceptionThrown = true;
        LOG.info("Exception message:" + ioe.getMessage());
      }
    }
    assertTrue(exceptionThrown, "Exception for partial record:");
  }

}
