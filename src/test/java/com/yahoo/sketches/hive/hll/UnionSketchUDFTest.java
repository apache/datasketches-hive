/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;

@SuppressWarnings("javadoc")
public class UnionSketchUDFTest {

  @Test
  public void nullInputs() {
    UnionSketchUDF udf = new UnionSketchUDF();
    BytesWritable result = udf.evaluate(null, null);
    HllSketch resultSketch = HllSketch.heapify(Memory.wrap(result.getBytes()));
    Assert.assertTrue(resultSketch.isEmpty());
    Assert.assertEquals(resultSketch.getEstimate(), 0.0);
  }

  @Test
  public void validSketches() {
    UnionSketchUDF udf = new UnionSketchUDF();

    HllSketch sketch1 = new HllSketch(SketchEvaluator.DEFAULT_LG_K);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    HllSketch sketch2 = new HllSketch(SketchEvaluator.DEFAULT_LG_K);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toCompactByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toCompactByteArray());

    BytesWritable result = udf.evaluate(input1, input2);

    HllSketch resultSketch = HllSketch.heapify(Memory.wrap(result.getBytes()));

    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.01);
  }

  @Test
  public void validSketchesExplicitParams () {
    UnionSketchUDF udf = new UnionSketchUDF();

    final int lgK = 10;
    final TgtHllType type = TgtHllType.HLL_6;

    HllSketch sketch1 = new HllSketch(10);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    HllSketch sketch2 = new HllSketch(10);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toCompactByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toCompactByteArray());

    BytesWritable result = udf.evaluate(input1, input2, lgK, type.toString());

    HllSketch resultSketch = HllSketch.heapify(Memory.wrap(result.getBytes()));

    Assert.assertEquals(resultSketch.getLgConfigK(), lgK);
    Assert.assertEquals(resultSketch.getTgtHllType(), type);
    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.02);
  }

}
