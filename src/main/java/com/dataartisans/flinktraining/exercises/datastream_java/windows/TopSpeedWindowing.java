package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * An example of grouped stream windowing where different eviction and trigger
 * policies can be used. A source fetches events from cars every 100 msec
 * containing their id, their current speed (kmh), overall elapsed distance (m)
 * and a timestamp. The streaming example triggers the top speed of each car
 * every x meters elapsed for the last y seconds.The result is sinked into elasticsearch6.x+
 */


/**
 *  create elasticsearch index and mapping type
 *
 *  index : curl -XPUT "http://localhost:9200/cars-idx"
 *  mapping type:
 *         curl -XPUT "http://localhost:9200/cars-idx/_mapping/top-speed" -d'
 *         {
 *          "top-speed" : {
 *            "properties" : {
 *               "carId": {"type": "integer"},
 *               "carData": {
 *                "properties":{
 *                 "carId": {"type": "integer"},
 *                 "speed": {"type": "integer"},
 *                 "distance": {"type": "double"},
 *                 "timestamp": {"type": "long"}
 *              	}
 *               }
 *             }
 *          }
 *         }'
 */

/**
 * Created by guanghui01.rong on 2019/5/30.
 */
public class TopSpeedWindowing {

	public final static String INDEX = "cars-idx";
	public final static String TYPE = "top-speed";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final List<HttpHost> hostList = new ArrayList<>();
		hostList.add(new HttpHost("127.0.0.1",9200,"http"));

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setGlobalJobParameters(params);
		env.getConfig().disableSysoutLogging();
		env.setParallelism(4);
		env.enableCheckpointing(30000L);
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.getCheckpointConfig().setFailOnCheckpointingErrors(false);

		@SuppressWarnings({"rawtypes", "serial"})
		DataStream<Tuple4<Integer, Integer, Double, Long>> carData;
		if (params.has("input")) {
			carData = env.readTextFile(params.get("input")).map(new ParseCarData());
		} else {
			System.out.println("Executing TopSpeedWindowing example with default input data set.");
			System.out.println("Use --input to specify file input.");
			carData = env.addSource(CarSource.create(100));
		}

		int evictionSec = 10;
		double triggerMeters = 50;
		DataStream<CarData> topSpeeds = carData
				.assignTimestampsAndWatermarks(new CarTimestamp())
				.keyBy(0)
				.window(GlobalWindows.create())
				.evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
				.trigger(DeltaTrigger.of(triggerMeters,
						new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public double getDelta(
									Tuple4<Integer, Integer, Double, Long> oldDataPoint,
									Tuple4<Integer, Integer, Double, Long> newDataPoint) {
								return newDataPoint.f2 - oldDataPoint.f2;
							}
						}, carData.getType().createSerializer(env.getConfig())))
				.maxBy(1)
				.flatMap(new FlatMapFunction<Tuple4<Integer, Integer, Double, Long>, CarData>() {
					@Override
					public void flatMap(Tuple4<Integer, Integer, Double, Long> value, Collector<CarData> out)
							throws Exception {
						out.collect(CarData.convertToPojo(value));
					}
				});



		//es builder
		ElasticsearchSink.Builder<CarData> esBuilder = new ElasticsearchSink.Builder<>(hostList,
				new ElasticsearchSinkFunction<CarData>() {

					public IndexRequest createIndexRequest(CarData carData){
						Map<String,Object> json = new HashMap();
						Map<String,Object> carJson = new HashMap<>();

						json.put("carId",carData.getCarId());

						carJson.put("carId",carData.getCarId());
						carJson.put("speed",carData.getSpeed());
						carJson.put("distance",carData.getDistance());
						carJson.put("timestamp",carData.getTimestamp());

						json.put("carData",carJson);

						return Requests.indexRequest()
								.index(INDEX)
								.type(TYPE)
								.id(carData.getCarId().toString())
								.source(json);

					}

					public UpdateRequest updateIndexRequest(CarData carData){
						Map<String,Object> json = new HashMap();
						Map<String,Object> carJson = new HashMap<>();

						json.put("carId",carData.getCarId());

						carJson.put("carId",carData.getCarId());
						carJson.put("speed",carData.getSpeed());
						carJson.put("distance",carData.getDistance());
						carJson.put("timestamp",carData.getTimestamp());

						json.put("carData",carJson);

						return new UpdateRequest(
								INDEX,
								TYPE,
								carData.getCarId().toString())
								.doc(json)
								.upsert(json);
					}

					@Override
					public void process(CarData carData, RuntimeContext context,
							RequestIndexer indexer) {
						indexer.add(createIndexRequest(carData));
						indexer.add(updateIndexRequest(carData));
					}
				});


//		configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
		esBuilder.setBulkFlushMaxActions(1);
		esBuilder.setFailureHandler(new ActionRequestFailureHandler() {
			@Override
			public void onFailure(ActionRequest request, Throwable failure, int restStatusCode, RequestIndexer indexer)
					throws Throwable {

				System.out.println("RestStatusCode = " + restStatusCode);
				if (request instanceof IndexRequest){
					Map<String, Object> json = new HashMap<>();
					json.put("data", ((IndexRequest) request).source());

					indexer.add(
							Requests.indexRequest()
									.index(INDEX)
									.type(TYPE)
									.id(((IndexRequest) request).id())
									.source(json));
				}else {
					throw new IllegalStateException("unexpected");
				}

			}
		});

/*		esBuilder.setRestClientFactory((restClientBuilder -> {
			restClientBuilder.setMaxRetryTimeoutMillis(5000);
			restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback(){
				@Override
				public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
					requestConfigBuilder.setRedirectsEnabled(true);
					return requestConfigBuilder;
				}
			});

		}));*/

	   // sink to stdout
//		topSpeeds.print();
		// sink to es
		topSpeeds.addSink(esBuilder.build());

		env.execute("CarTopSpeedWindowingSinkToEsExample");
	}

	// USER FUNCTIONS
	// *************************************************************************

	private static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {

		private static final long serialVersionUID = 1L;
		private Integer[] speeds;
		private Double[] distances;

		private Random rand = new Random();

		private volatile boolean isRunning = true;

		private CarSource(int numOfCars) {
			speeds = new Integer[numOfCars];
			distances = new Double[numOfCars];
			Arrays.fill(speeds, 50);
			Arrays.fill(distances, 0d);
		}

		public static CarSource create(int cars) {
			return new CarSource(cars);
		}

		@Override
		public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx) throws Exception {

			while (isRunning) {
				Thread.sleep(100);
				for (int carId = 0; carId < speeds.length; carId++) {
					if (rand.nextBoolean()) {
						speeds[carId] = Math.min(100, speeds[carId] + 5);
					} else {
						speeds[carId] = Math.max(0, speeds[carId] - 5);
					}
					distances[carId] += speeds[carId] / 3.6d;
					Tuple4<Integer, Integer, Double, Long> record = new Tuple4<>(carId,
							speeds[carId], distances[carId], System.currentTimeMillis());
					ctx.collect(record);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class ParseCarData extends RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple4<Integer, Integer, Double, Long> map(String record) {
			String rawData = record.substring(1, record.length() - 1);
			String[] data = rawData.split(",");
			return new Tuple4<>(Integer.valueOf(data[0]), Integer.valueOf(data[1]), Double.valueOf(data[2]), Long.valueOf(data[3]));
		}
	}

	private static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
			return element.f3;
		}
	}


	private static class CarData implements Serializable {
		private Integer carId;
		private Integer speed;
		private Double distance;
		private Long timestamp;

		public CarData(Integer carId, Integer speed, Double distance, Long timestamp) {
			this.carId = carId;
			this.speed = speed;
			this.distance = distance;
			this.timestamp = timestamp;
		}

		public Integer getCarId() {
			return carId;
		}

		public Integer getSpeed() {
			return speed;
		}

		public Double getDistance() {
			return distance;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public static CarData convertToPojo(Tuple4<Integer, Integer, Double, Long> element){
			return new CarData(element.f0,element.f1,element.f2,element.f3);
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("CarData{");
			sb.append("carId=").append(carId);
			sb.append(", speed=").append(speed);
			sb.append(", distance=").append(distance);
			sb.append(", timestamp=").append(timestamp);
			sb.append('}');
			return sb.toString();
		}
	}
}
