/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink;

import java.io.File;
import java.util.UUID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkSource extends AbstractTestBase {

  private static final String[] WORDS = new String[]{
      "hello world",
      "foo foo foo bar bar",
      "hello foo",
      "hello bar",
      "bar word"
  };

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String resultPath = null;
  private File resultFile = null;

  @Before
  public void before() throws Exception {
    final File folder = tempFolder.newFolder();
    resultFile = new File(folder, UUID.randomUUID().toString());
    resultPath = resultFile.toURI().toString();
  }

  @Test
  public void testWordCount() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> text = env.fromElements(WORDS);

    DataStream<Tuple2<String, Integer>> counts = text
        .flatMap(new Tokenizer())
        .keyBy(0)
        .sum(1);

    // Output the data stream to stdout.
    counts.writeAsText(resultPath);

    // Execute the program.
    env.execute("Streaming Wordcount");

    String expectedResult = "(bar,1)\n(bar,2)\n(bar,3)\n(bar,4)\n" +
        "(foo,1)\n(foo,2)\n(foo,3)\n(foo,4)\n" +
        "(hello,1)\n(hello,2)\n(hello,3)\n" +
        "(word,1)\n(world,1)";
    compareResultsByLinesInMemory(expectedResult, resultPath);
  }

  /**
   * Implements the string tokenizer that splits sentences into words as a
   * user-defined FlatMapFunction. The function takes a line (String) and
   * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
   * Integer>}).
   */
  public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
      // normalize and split the line
      String[] tokens = value.toLowerCase().split("\\W+");

      // emit the pairs
      for (String token : tokens) {
        if (token.length() > 0) {
          out.collect(new Tuple2<>(token, 1));
        }
      }
    }
  }
}
