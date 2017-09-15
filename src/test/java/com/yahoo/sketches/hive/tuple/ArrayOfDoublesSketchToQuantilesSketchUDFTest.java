/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

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
    DoublesSketch qs = DoublesSketch.wrap(Memory.wrap(result.getBytes()));
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
    DoublesSketch qs = DoublesSketch.wrap(Memory.wrap(result.getBytes()));
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
    DoublesSketch qs = DoublesSketch.wrap(Memory.wrap(result.getBytes()));
    Assert.assertFalse(qs.isEmpty());
    Assert.assertEquals(qs.getK(), k);
    Assert.assertEquals(qs.getMinValue(), 2.0);
    Assert.assertEquals(qs.getMaxValue(), 20.0);
  }

}
