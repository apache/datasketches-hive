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

package org.apache.datasketches.hive.quantiles;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

@SuppressWarnings("javadoc")
public class GetCdfFromDoublesSketchUDFTest {

  @Test
  public void nullSketch() {
    List<Double> result = new GetCdfFromDoublesSketchUDF().evaluate(null, 0.0);
    Assert.assertNull(result);
  }

  @Test
  public void emptyListOfSplitPoints() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    List<Double> result = new GetCdfFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 1.0);
  }

  @Test
  public void emptySketch() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    List<Double> result = new GetCdfFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 0.0);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    List<Double> result = new GetCdfFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 1.0, 3.0, 4.0);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.5);
    Assert.assertEquals(result.get(2), 0.75);
    Assert.assertEquals(result.get(3), 1.0);
  }

}
