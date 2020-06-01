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

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToQuantilesSketchUDFTest {

  @Test
  public void nullSketch() {
    BytesWritable result = new ArrayOfDoublesSketchToQuantilesSketchUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void emptySketchZeroColumn() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    new ArrayOfDoublesSketchToQuantilesSketchUDF().evaluate(
      new BytesWritable(sketch.compact().toByteArray()),
      0
    );
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void emptySketchColumnOutOfRange() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    new ArrayOfDoublesSketchToQuantilesSketchUDF().evaluate(
      new BytesWritable(sketch.compact().toByteArray()),
      2
    );
  }

  @Test
  public void emptySketch() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    BytesWritable result = new ArrayOfDoublesSketchToQuantilesSketchUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    DoublesSketch qs = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(result));
    Assert.assertTrue(qs.isEmpty());
  }

  @Test
  public void nonEmptySketchExplicitColumn() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {1});
    sketch.update(2, new double[] {10});
    BytesWritable result = new ArrayOfDoublesSketchToQuantilesSketchUDF().evaluate(
      new BytesWritable(sketch.compact().toByteArray()),
      1
    );
    Assert.assertNotNull(result);
    DoublesSketch qs = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(result));
    Assert.assertFalse(qs.isEmpty());
    Assert.assertEquals(qs.getMinValue(), 1.0);
    Assert.assertEquals(qs.getMaxValue(), 10.0);
  }

  @Test
  public void nonEmptySketchWithTwoColumnsExplicitK() {
    int k = 256;
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch.update(1, new double[] {1.0, 2.0});
    sketch.update(2, new double[] {10.0, 20.0});
    BytesWritable result = new ArrayOfDoublesSketchToQuantilesSketchUDF().evaluate(
      new BytesWritable(sketch.compact().toByteArray()),
      2,
      k
    );
    Assert.assertNotNull(result);
    DoublesSketch qs = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(result));
    Assert.assertFalse(qs.isEmpty());
    Assert.assertEquals(qs.getK(), k);
    Assert.assertEquals(qs.getMinValue(), 2.0);
    Assert.assertEquals(qs.getMaxValue(), 20.0);
  }

}
