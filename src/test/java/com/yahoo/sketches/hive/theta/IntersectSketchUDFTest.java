/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;


import org.apache.hadoop.io.BytesWritable;
import org.testng.annotations.Test;

import com.yahoo.sketches.hive.theta.IntersectSketchUDF;

import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import static org.testng.AssertJUnit.assertEquals;

public class IntersectSketchUDFTest  {

  // test for null sketches
  @Test
  public void testEvaluateNull() {
    IntersectSketchUDF testObject = new IntersectSketchUDF();

    BytesWritable intermResult1 = testObject.evaluate(null, null, null);
    BytesWritable intermResult2 = testObject.evaluate(null, null);

    Memory mem1 = new NativeMemory(intermResult1.getBytes());
    Memory mem2 = new NativeMemory(intermResult2.getBytes());

    Sketch testResult1 = Sketches.heapifySketch(mem1);
    Sketch testResult2 = Sketches.heapifySketch(mem2);

    assertEquals(0.0, testResult1.getEstimate());
    assertEquals(0.0, testResult2.getEstimate());
  }

  // test for empty sketches
  @Test
  public void testEvaluateEmpty() {
    IntersectSketchUDF testObject = new IntersectSketchUDF();

    BytesWritable intermResult1 = testObject.evaluate(new BytesWritable(), new BytesWritable(), null);
    BytesWritable intermResult2 = testObject.evaluate(new BytesWritable(), new BytesWritable());

    Memory mem1 = new NativeMemory(intermResult1.getBytes());
    Memory mem2 = new NativeMemory(intermResult2.getBytes());

    Sketch testResult1 = Sketches.heapifySketch(mem1);
    Sketch testResult2 = Sketches.heapifySketch(mem2);

    assertEquals(0.0, testResult1.getEstimate());
    assertEquals(0.0, testResult2.getEstimate());
  }

  // test valid sketches
  @Test
  public void testEvaluateValidSketch() {
    IntersectSketchUDF testObject = new IntersectSketchUDF();
    
    UpdateSketch sketch1 = Sketches.updateSketchBuilder().build(1024);
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }
    
    UpdateSketch sketch2 = Sketches.updateSketchBuilder().build(1024);
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact(true, null).toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact(true, null).toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2);
    
    Sketch result = Sketches.heapifySketch(new NativeMemory(output.getBytes()));
    
    assertEquals(28.0, result.getEstimate());
  }
}
