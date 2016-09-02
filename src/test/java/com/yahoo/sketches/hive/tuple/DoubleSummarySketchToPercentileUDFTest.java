/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;

public class DoubleSummarySketchToPercentileUDFTest {

  @Test
  public void nullSketch() {
    Double result = new DoubleSummarySketchToPercentileUDF().evaluate(null, 0);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    Double result = new DoubleSummarySketchToPercentileUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()), 0.0);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Double.POSITIVE_INFINITY);
  }

  @Test
  public void normalCase() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    int iterations = 100000;
    for (int i = 0; i < iterations; i++) sketch.update(i, (double) i);
    for (int i = 0; i < iterations; i++) sketch.update(i, (double) i);
    Double result = new DoubleSummarySketchToPercentileUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()), 50.0);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, iterations, iterations * 0.02);
  }

}
