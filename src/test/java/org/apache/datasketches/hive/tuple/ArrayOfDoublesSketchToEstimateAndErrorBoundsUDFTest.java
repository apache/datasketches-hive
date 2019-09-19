/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.tuple;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToEstimateAndErrorBoundsUDFTest {

  @Test
  public void nullSketch() {
    List<Double> result = new ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    List<Double> result = new ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.0);
    Assert.assertEquals(result.get(2), 0.0);
  }

  @Test
  public void exactMode() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {0});
    sketch.update(2, new double[] {0});
    List<Double> result = new ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 2.0);
    Assert.assertEquals(result.get(1), 2.0);
    Assert.assertEquals(result.get(2), 2.0);
  }

  @Test
  public void estimationMode() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    int numKeys = 10000; // to saturate the sketch with default number of nominal entries (4K)
    for (int i = 0; i < numKeys; i++ ) {
      sketch.update(i, new double[] {0});
    }
    List<Double> result = new ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    double estimate = result.get(0);
    double lowerBound = result.get(1);
    double upperBound = result.get(2);
    Assert.assertEquals(estimate, numKeys, numKeys * 0.04);
    Assert.assertEquals(lowerBound, numKeys, numKeys * 0.04);
    Assert.assertEquals(upperBound, numKeys, numKeys * 0.04);
    Assert.assertTrue(lowerBound < estimate);
    Assert.assertTrue(upperBound > estimate);
  }

}
