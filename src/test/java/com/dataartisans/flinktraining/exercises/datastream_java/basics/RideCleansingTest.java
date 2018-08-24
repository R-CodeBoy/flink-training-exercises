/*
 * Copyright 2018 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.basics;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RideCleansingTest extends TaxiRideTestBase<TaxiRide> {

	static Testable javaExercise = () -> RideCleansingExercise.main(new String[]{});

	@Test
	public void testCleansingFilter() throws Exception {

		TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
		TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
		TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
		TaxiRide atNorthPole = testRide(0, 90, 0, 90);

		TestRideSource source = new TestRideSource(atPennStation, toThePole, fromThePole, atNorthPole);

//		assertEquals(Lists.newArrayList(atPennStation), results(source));
//		assertEquals(Lists.newArrayList(toThePole), results(source));
//		assertEquals(Lists.newArrayList(fromThePole), results(source));
//		assertEquals(Lists.newArrayList(atNorthPole), results(source));
		assertEquals(Lists.newArrayList(fromThePole,atNorthPole,toThePole), results(source));
	}

	private TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
		return new TaxiRide(1L, true, new DateTime(0), new DateTime(0),
				startLon, startLat, endLon, endLat, (short)1, 0, 0);
	}

	protected List<?> results(TestRideSource source) throws Exception {
		Testable javaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_java.basics.RideCleansingSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), javaExercise, javaSolution);
	}
/*
	public static void main(String[] args) {
		System.out.println(new DateTime(0));
	}*/

}