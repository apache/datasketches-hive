/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;

public class DoubleSummarySketchToEstimatesUDFTest {

  @Test
  public void nullSketch() {
    List<Double> result = new DoubleSummarySketchToEstimatesUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    List<Double> result = new DoubleSummarySketchToEstimatesUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.0);
  }

  @Test
  public void normalCase() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch.update(1, 1.0);
    sketch.update(2, 1.0);
    List<Double> result = new DoubleSummarySketchToEstimatesUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 2.0);
    Assert.assertEquals(result.get(1), 2.0);
  }

}
