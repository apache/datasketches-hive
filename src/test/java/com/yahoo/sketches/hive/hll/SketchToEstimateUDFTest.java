/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.hll.HllSketch;

@SuppressWarnings("javadoc")
public class SketchToEstimateUDFTest {

  @Test
  public void nullSketch() {
    Double result = new SketchToEstimateUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    HllSketch sketch = new HllSketch(10);
    Double result = new SketchToEstimateUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, 0.0);
  }

  @Test
  public void normalCase() {
    HllSketch sketch = new HllSketch(10);
    sketch.update(1);
    sketch.update(2);
    Double result = new SketchToEstimateUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, 2.0, 0.01);
  }

}
