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

package org.apache.datasketches.hive.hll;

import java.util.List;

import org.apache.datasketches.hll.HllSketch;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class SketchToEstimateAndErrorBoundsUDFTest {

  @Test
  public void nullSketch() {
    final List<Double> result = new SketchToEstimateAndErrorBoundsUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    final HllSketch sketch = new HllSketch(12);
    final List<Double> result = new SketchToEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals((double)result.get(0), 0.0);
    Assert.assertEquals((double)result.get(1), 0.0);
    Assert.assertEquals((double)result.get(2), 0.0);
  }

  @Test
  public void normalCase() {
    final HllSketch sketch = new HllSketch(12);
    sketch.update(1);
    sketch.update(2);
    List<Double> result = new SketchToEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 2.0, 0.01);
    Assert.assertTrue(result.get(1) <= 2.0);
    Assert.assertTrue(result.get(2) >= 2.0);
  }

  @Test
  public void normalCaseWithKappa() {
    final HllSketch sketch = new HllSketch(12);
    sketch.update(1);
    sketch.update(2);
    List<Double> result = new SketchToEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()), 3);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 2.0, 0.01);
    Assert.assertTrue(result.get(1) <= 2.0);
    Assert.assertTrue(result.get(2) >= 2.0);
  }

}
