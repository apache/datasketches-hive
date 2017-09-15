/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

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
