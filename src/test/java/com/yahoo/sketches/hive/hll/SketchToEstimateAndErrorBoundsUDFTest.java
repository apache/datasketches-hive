/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.hll.HllSketch;

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
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.0);
    Assert.assertEquals(result.get(2), 0.0);
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
