package com.yelun.flink.exercise;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

/**
 * Created by guanghui01.rong on 2019/6/11. 
 */
public class WordCountTransform {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input")).setParallelism(1);
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.fromElements(WordCountData.WORDS).setParallelism(1);
		}

		final int windowSize = 10;
		final int slideSize = 5;

		DataStream<Tuple2<String,Integer>>  counts = text.flatMap(new Tokenizer())
		.setParallelism(4).slotSharingGroup("flatmap_sg")
		.keyBy(0)
		.countWindow(windowSize,slideSize)
		.sum(1).setParallelism(2).slotSharingGroup("sum_sg");


		counts.print().setParallelism(2);

		env.execute("flink job execute graph demo");
	}


	static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W+");

			for (int i = 0; i < tokens.length; i++) {
				out.collect(new Tuple2<>(tokens[i], 1));
			}
		}
	}
}
