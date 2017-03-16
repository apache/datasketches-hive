/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;

public class UnionSketchUDFTest {

  @Test
  public void evaluateNull() {
    UnionSketchUDF testObject = new UnionSketchUDF();
    BytesWritable intermResult = testObject.evaluate(null, null);
    Memory mem = new NativeMemory(intermResult.getBytes());
    Sketch testResult = Sketches.heapifySketch(mem);
    Assert.assertEquals(testResult.getEstimate(), 0.0);
  }

  @Test
  public void testEvaluateEmpty() {
    UnionSketchUDF testObject = new UnionSketchUDF();
    BytesWritable intermResult = testObject.evaluate(new BytesWritable(), new BytesWritable());
    Memory mem = new NativeMemory(intermResult.getBytes());
    Sketch testResult = Sketches.heapifySketch(mem);
    Assert.assertEquals(testResult.getEstimate(), 0.0);
  }

  @Test
  public void evaluateValidSketch () {
    UnionSketchUDF testObject = new UnionSketchUDF();

    UpdateSketch sketch1 = Sketches.updateSketchBuilder().build(1024);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().build(1024);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact().toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact().toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2);

    Sketch result = Sketches.heapifySketch(new NativeMemory(output.getBytes()));

    Assert.assertEquals(256.0, result.getEstimate());
  }

  @Test
  public void evaluateValidSketchExplicitSizeAndSeed () {
    UnionSketchUDF testObject = new UnionSketchUDF();

    final long seed = 1;
    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setSeed(seed).build(1024);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setSeed(seed).build(1024);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact().toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact().toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2, 128, seed);

    Sketch result = Sketches.heapifySketch(new NativeMemory(output.getBytes()), seed);

    Assert.assertEquals(256.0, result.getEstimate(), 256 * 0.02);
    Assert.assertTrue(result.getRetainedEntries(true) <= 128.0);
  }

}
