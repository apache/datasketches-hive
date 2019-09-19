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

package org.apache.datasketches.hive.cpc;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.cpc.CpcSketch;

@SuppressWarnings("javadoc")
public class GetEstimateAndErrorBoundsUDFTest {

  @Test
  public void nullSketch() {
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    final CpcSketch sketch = new CpcSketch(12);
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.0);
    Assert.assertEquals(result.get(2), 0.0);
  }

  @Test
  public void nonEmptySketchDefaultParams() {
    final CpcSketch sketch = new CpcSketch(12);
    sketch.update(1);
    sketch.update(2);
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.get(0), 2.0, 0.01);
    Assert.assertTrue(result.get(1) <= 2.0);
    Assert.assertTrue(result.get(2) >= 2.0);
  }

  @Test
  public void nonEmptySketchExplicitKappa() {
    final CpcSketch sketch = new CpcSketch(12);
    sketch.update(1);
    sketch.update(2);
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toByteArray()), 2);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.get(0), 2.0, 0.01);
    Assert.assertTrue(result.get(1) <= 2.0);
    Assert.assertTrue(result.get(2) >= 2.0);
  }

  @Test
  public void customSeed() {
    final long seed = 123;
    final CpcSketch sketch = new CpcSketch(12, seed);
    sketch.update(1);
    sketch.update(2);
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toByteArray()), 1, seed);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.get(0), 2.0, 0.01);
    Assert.assertTrue(result.get(1) <= 2.0);
    Assert.assertTrue(result.get(2) >= 2.0);
  }

}
