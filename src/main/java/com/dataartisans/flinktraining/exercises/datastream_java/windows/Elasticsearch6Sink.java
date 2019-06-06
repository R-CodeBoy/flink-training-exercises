package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/** create elasticsearch index and mapping type
 *  index : PUT /es-test
 *  mapping type:
 *   PUT /es-test/_mapping/es-seeds
 *  {
 *      "es-seeds" : {
 *      "properties" : {
 *       "data": {"type": "text"}
 *     }
 *  }
 * }
 *
 *
 *
 */

/**
 * Created by guanghui01.rong on 2019/6/3. 
 */
public class Elasticsearch6Sink {
	private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6Sink.class);

	public static void main(String[] args) throws Exception {

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 3) {
			System.out.println("Missing parameters!\n" +
					"Usage: --numRecords <numRecords> --index <index> --type <type>");
			logger.error("error parma");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(10000);
		env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> source = env.generateSequence(0, parameterTool.getInt("numRecords") - 1)
				.flatMap(new FlatMapFunction<Long, Tuple2<String, String>>() {
					@Override
					public void flatMap(Long value, Collector<Tuple2<String, String>> out) {
						final String key = String.valueOf(value + 2);
						final String message = "message #" + value;
						out.collect(Tuple2.of(key, message + "first insert #1"));
						out.collect(Tuple2.of(key, message + "second update #2"));
					}
				});

		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

		ElasticsearchSink.Builder<Tuple2<String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				(Tuple2<String, String> element, RuntimeContext ctx, RequestIndexer indexer) -> {
					indexer.add(createIndexRequest(element.f1, parameterTool));
					indexer.add(createUpdateRequest(element, parameterTool));
				});

		esSinkBuilder.setFailureHandler(
				new CustomFailureHandler(parameterTool.getRequired("index"), parameterTool.getRequired("type")));

		// this instructs the sink to emit after every element, otherwise they would be buffered
		esSinkBuilder.setBulkFlushMaxActions(1);

//		source.print();

		source.addSink(esSinkBuilder.build());

		env.execute("Elasticsearch 6.x end to end sink example");
	}

	private static class CustomFailureHandler implements ActionRequestFailureHandler {

		private static final long serialVersionUID = 942269087742453482L;

		private final String index;
		private final String type;

		CustomFailureHandler(String index, String type) {
			this.index = index;
			this.type = type;
		}

		@Override
		public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
			if (action instanceof IndexRequest) {
				Map<String, Object> json = new HashMap<>();
				json.put("data", ((IndexRequest) action).source());

				indexer.add(
						Requests.indexRequest()
								.index(index)
								.type(type)
								.id(((IndexRequest) action).id())
								.source(json));
			} else {
				throw new IllegalStateException("unexpected");
			}
		}
	}

	private static IndexRequest createIndexRequest(String element, ParameterTool parameterTool) {
		Map<String, Object> json = new HashMap<>();
		json.put("data", element);

		String index;
		String type;

		if (element.startsWith("message #15")) {
			index = ":intentional invalid index:";
			type = ":intentional invalid type:";
		} else {
			index = parameterTool.getRequired("index");
			type = parameterTool.getRequired("type");
		}

		return Requests.indexRequest()
				.index(index)
				.type(type)
				.id(element)
				.source(json);
	}

	private static UpdateRequest createUpdateRequest(Tuple2<String, String> element, ParameterTool parameterTool) {
		Map<String, Object> json = new HashMap<>();
		json.put("data", element.f1);

		return new UpdateRequest(
				parameterTool.getRequired("index"),
				parameterTool.getRequired("type"),
				element.f0)
				.doc(json)
				.upsert(json);
	}
}
