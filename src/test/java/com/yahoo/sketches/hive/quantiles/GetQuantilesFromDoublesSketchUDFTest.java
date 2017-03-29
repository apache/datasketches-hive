/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

public class GetQuantilesFromDoublesSketchUDFTest {

  @Test
  public void nullSketch() {
    List<Double> result = new GetQuantilesFromDoublesSketchUDF().evaluate(null, 0.0);
    Assert.assertNull(result);
  }

  @Test
  public void emptyListOfFractions() {
    UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    List<Double> result = new GetQuantilesFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void fractionsNormalCase() {
    UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    List<Double> result = new GetQuantilesFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 0.0, 0.5, 1.0);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 1.0);
    Assert.assertEquals(result.get(1), 2.0);
    Assert.assertEquals(result.get(2), 3.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void evenlySpacedZero() {
    UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().build();
    new GetQuantilesFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 0);
  }

  @Test
  public void evenlySpacedNormalCase() {
    UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    List<Double> result = new GetQuantilesFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 3);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 1.0);
    Assert.assertEquals(result.get(1), 2.0);
    Assert.assertEquals(result.get(2), 3.0);
  }

}
