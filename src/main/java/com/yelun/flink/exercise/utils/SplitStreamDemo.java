package com.yelun.flink.exercise.utils;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * *Created by guanghui01.rong on 2019/6/13.
 */
public class SplitStreamDemo {
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		SplitStream<Long> split1 = env.generateSequence(1, 11).split(new ThresholdSelector(6));
		// stream11 should be [1,2,3,4,5]

		DataStream<Long> stream11 = split1.select("Less");
		split1.print();

		DataStream<Long> split21 = stream11
				//		.map(new MapFunction<Long, Long>() {
				//			@Override
				//			public Long map(Long value) throws Exception {
				//				return value;
				//			}
				//		})
//				.split(new ThresholdSelector(3));
				.filter(value -> value.compareTo(3L) < 0);
//		DataStream<Long> stream21 = split2.select("Less");
		// stream21 should be [1,2]
		split21.print();

		env.execute();

	}

	private static class ThresholdSelector implements OutputSelector<Long> {
		private int threshold;

		public ThresholdSelector(int threshold) {
			this.threshold = threshold;
		}

		@Override
		public Iterable<String> select(Long value) {
			if (value <= threshold) {
				return Collections.singletonList("LESS");
			}
			else {
				return Collections.singletonList("GreaterEqual");
			}
		}
	}
}
