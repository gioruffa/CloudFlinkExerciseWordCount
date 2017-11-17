package eu.gioruffa.flink;

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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class WordCount2 {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//connect with the source
        DataStreamSource<String> stream2 = env.readTextFile("/home/giorgio/UniData/Cloud/11-0.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream3 = stream2.flatMap(
            new FlatMapFunction<String, Tuple2<String,Integer>>() {
                /**
                 * The collector is needed because you can emit multiple or no tuples in the flat map
                 * @param s
                 * @param collector
                 * @throws Exception
                 */
                @Override
                public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    for (String word : s.split(" "))
                    {
                        collector.collect(new Tuple2<String, Integer>(word,1));
                    }
                }
            }
        );

        /**
         * 0 is the index of the field in the stream to use
         * Can make composite keys with 0,1,2,3 (... operator)
         */
        KeyedStream<Tuple2<String, Integer>, Tuple> stream4 = stream3.keyBy(0);//key by world

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream5 = stream4.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            /**
             *
             * @param accumulator -> result up to now
             * @param actualTuple -> new value
             * @return
             * @throws Exception
             */
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> actualTuple) throws Exception {
                //since we are using a partitioned stream -> no need to check the String key
                Tuple2<String,Integer> newAccumulator = new Tuple2<>(
                        accumulator.f0, accumulator.f1 + actualTuple.f1
                );
                return newAccumulator; //ATTENTION! THIS EMIT THE THE TOUPLE IN THE STREAM (will be processed by the next stream!)
                //AND AT THE SAME TIME IS USED AS THE accumulator IN THE NEW REDUCE CALL!
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream6 = stream5.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f1  > 10;
            }
        });

        //save results
        stream6.writeAsCsv("/tmp/count.result");

        try
        {
            env.execute(); //always needed, but not if you use the print statement (automatically calls the execute)
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

    }



}
