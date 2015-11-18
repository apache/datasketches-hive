/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

/**
 * Hive estimate sketch udf.
 *
 */
public class SampleSketchUDF extends UDF {
  public static final int DEFAULT_SIZE = 16384;

  /**
   * Main logic called by hive, produces new sketch from original using
   * specified size and sampling probablility.
   * 
   * @param binarySketch
   *          sketch to be sampled passed in as bytes writable.
   * @param sketchSize 
   *          Size to use for the new sketch.
   *          This must be a power of 2 and larger than 16. If zero, DEFAULT is used.
   * @param probability
   *          The sampling probability to use for the new sketch. 
   *          Should be greater than zero and less than or equal to 1.0 
   * @return The sampled sketch encoded as a BytesWritable
   */
  public BytesWritable evaluate(BytesWritable binarySketch, int sketchSize, float probability) {
    
    // Null checks
    if (binarySketch == null) {
      return null;
    }
    
    byte[] serializedSketch = binarySketch.getBytes();
    
    if (serializedSketch.length <= 8) {
      return null;
    }
    
    //  The builder will catch errors with improper sketchSize or probability
    Union union = SetOperation.builder().setP(probability).buildUnion(sketchSize);

    union.update(new NativeMemory(serializedSketch)); //Union can accept Memory object directly

    Sketch intermediateSketch = union.getResult(false, null); //to CompactSketch(unordered, on-heap)
    byte[] resultSketch = intermediateSketch.toByteArray();

    BytesWritable result = new BytesWritable();
    result.set(resultSketch, 0, resultSketch.length);

    return result;
  }
}
