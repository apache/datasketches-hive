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
public class ArrayOfDoublesSketchesTTestUDFTest {

  @Test
  public void nullSketches() {
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(null, null);
    Assert.assertNull(result);
  }

  @Test
  public void nullSketchA() {
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(
      null,
      new BytesWritable(sketchB.compact().toByteArray())
    );
    Assert.assertNull(result);
  }

  @Test
  public void nullSketchB() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(
      new BytesWritable(sketchA.compact().toByteArray()),
      null
    );
    Assert.assertNull(result);
  }

  @Test
  public void emptySketchA() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(1, new double[] {1});
    sketchB.update(2, new double[] {1});
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(
      new BytesWritable(sketchA.compact().toByteArray()),
      new BytesWritable(sketchB.compact().toByteArray())
    );
    Assert.assertNull(result);
  }

  @Test
  public void emptySketchB() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {1});
    sketchA.update(2, new double[] {1});
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(
      new BytesWritable(sketchA.compact().toByteArray()),
      new BytesWritable(sketchB.compact().toByteArray())
    );
    Assert.assertNull(result);
  }

  @Test
  public void oneEntrySketchA() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {1});
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(1, new double[] {1});
    sketchB.update(2, new double[] {1});
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(
      new BytesWritable(sketchA.compact().toByteArray()),
      new BytesWritable(sketchB.compact().toByteArray())
    );
    Assert.assertNull(result);
  }

  @Test
  public void oneEntrySketchB() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {1});
    sketchA.update(2, new double[] {1});
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(1, new double[] {1});
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(
      new BytesWritable(sketchA.compact().toByteArray()),
      new BytesWritable(sketchB.compact().toByteArray())
    );
    Assert.assertNull(result);
  }

  @Test
  public void twoEntriesInBothSketchesNoDifference() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {0.99});
    sketchA.update(2, new double[] {1.01});
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(1, new double[] {0.99});
    sketchB.update(2, new double[] {1.01});
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(
      new BytesWritable(sketchA.compact().toByteArray()),
      new BytesWritable(sketchB.compact().toByteArray())
    );
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 1.0, 0.01);
  }

  @Test
  public void twoEntriesInBothSketchesLargeDifference() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {0.99});
    sketchA.update(2, new double[] {1.01});
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(1, new double[] {1.99});
    sketchB.update(2, new double[] {2.01});
    List<Double> result = new ArrayOfDoublesSketchesTTestUDF().evaluate(
      new BytesWritable(sketchA.compact().toByteArray()),
      new BytesWritable(sketchB.compact().toByteArray())
    );
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 0.0, 0.01);
  }

}
