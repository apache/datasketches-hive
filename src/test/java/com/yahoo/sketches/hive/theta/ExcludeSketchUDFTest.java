/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.testng.annotations.Test;

import com.yahoo.sketches.hive.theta.ExcludeSketchUDF;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;
import static org.testng.AssertJUnit.assertEquals;

public class ExcludeSketchUDFTest {

  // test for null first sketch
  @Test
  public void testEvaluateNull() {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    BytesWritable intermResult1 = testObject.evaluate(null, null, null);
    BytesWritable intermResult2 = testObject.evaluate(null, null);

    Memory mem1 = new NativeMemory(intermResult1.getBytes());
    Memory mem2 = new NativeMemory(intermResult2.getBytes());

    Sketch testResult1 = Sketches.heapifySketch(mem1);
    Sketch testResult2 = Sketches.heapifySketch(mem2);
    
    assertEquals(0.0, testResult1.getEstimate());
    assertEquals(0.0, testResult2.getEstimate());
  }

  // test for empty first sketch
  @Test
  public void testEvaluateEmpty() {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    BytesWritable intermResult1 = testObject.evaluate(new BytesWritable(), new BytesWritable(), null);
    BytesWritable intermResult2 = testObject.evaluate(new BytesWritable(), new BytesWritable());

    Memory mem1 = new NativeMemory(intermResult1.getBytes());
    Memory mem2 = new NativeMemory(intermResult2.getBytes());

    Sketch testResult1 = Sketches.heapifySketch(mem1);
    Sketch testResult2 = Sketches.heapifySketch(mem2);

    assertEquals(0.0, testResult1.getEstimate());
    assertEquals(0.0, testResult2.getEstimate());
  }
  
  // test valid input sketches
  @Test
  public void testEvaluateValidSketch () {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();
    
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
    
    assertEquals(100.0, result.getEstimate());
  }

  // test valid input sketches
  @Test
  public void testEvaluateValidSketchWithDefaultSeed () {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();
    
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
    
    BytesWritable output = testObject.evaluate(input1, input2, new LongWritable(DEFAULT_UPDATE_SEED));
    
    Sketch result = Sketches.heapifySketch(new NativeMemory(output.getBytes()));
    
    assertEquals(100.0, result.getEstimate());
  }

}
