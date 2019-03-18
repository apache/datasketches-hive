/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.cpc.CpcSketch;

public class UnionSketchUDFTest {

  @Test
  public void nullInputs() {
    UnionSketchUDF udf = new UnionSketchUDF();
    BytesWritable result = udf.evaluate(null, null);
    CpcSketch resultSketch = CpcSketch.heapify(Memory.wrap(result.getBytes()));
    Assert.assertTrue(resultSketch.isEmpty());
    Assert.assertEquals(resultSketch.getEstimate(), 0.0);
  }

  @Test
  public void validSketches() {
    UnionSketchUDF udf = new UnionSketchUDF();

    CpcSketch sketch1 = new CpcSketch(SketchEvaluator.DEFAULT_LG_K);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    CpcSketch sketch2 = new CpcSketch(SketchEvaluator.DEFAULT_LG_K);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toByteArray());

    BytesWritable result = udf.evaluate(input1, input2);

    CpcSketch resultSketch = CpcSketch.heapify(Memory.wrap(result.getBytes()));

    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.02);
  }

  @Test
  public void validSketchesExplicitLgK () {
    UnionSketchUDF udf = new UnionSketchUDF();

    final int lgK = 10;

    CpcSketch sketch1 = new CpcSketch(lgK);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    CpcSketch sketch2 = new CpcSketch(lgK);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toByteArray());

    BytesWritable result = udf.evaluate(input1, input2, lgK);

    CpcSketch resultSketch = CpcSketch.heapify(Memory.wrap(result.getBytes()));

    Assert.assertEquals(resultSketch.getLgK(), lgK);
    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.02);
  }

  @Test
  public void validSketchesExplicitLgKAndSeed () {
    UnionSketchUDF udf = new UnionSketchUDF();

    final int lgK = 10;
    final long seed = 123;

    CpcSketch sketch1 = new CpcSketch(lgK, seed);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    CpcSketch sketch2 = new CpcSketch(lgK, seed);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toByteArray());

    BytesWritable result = udf.evaluate(input1, input2, lgK, seed);

    CpcSketch resultSketch = CpcSketch.heapify(Memory.wrap(result.getBytes()), seed);

    Assert.assertEquals(resultSketch.getLgK(), lgK);
    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.02);
  }

}
