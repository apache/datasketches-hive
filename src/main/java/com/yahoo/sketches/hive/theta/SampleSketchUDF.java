/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

/**
 * Hive estimate sketch udf. V4
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
   *          Size to use for the new sketch
   * @param probability
   *          The sampling probability to use for the new sketch
   * @return The sampled sketch encoded as a BytesWritable
   */
  public BytesWritable evaluate(BytesWritable binarySketch, int sketchSize, double probability) {
    int sketch_size = DEFAULT_SIZE;
    double sampling_probability = 1.0;

    if (binarySketch == null) {
      return null;
    }

    byte[] serializedSketch = binarySketch.getBytes();

    if (serializedSketch.length <= 8) {
      return null;
    }

    if (sketchSize > 0) {
      sketch_size = sketchSize;
    }
    if (probability > 0.0 && probability < 1.0) {
      sampling_probability = probability;
    }

    NativeMemory memorySketch = new NativeMemory(serializedSketch);

    Sketch heapSketch = Sketch.heapify(memorySketch);

    Union union = (Union) SetOperation.builder().setP((float) sampling_probability).build(sketch_size, Family.UNION);

    union.update(heapSketch);

    Sketch intermediateSketch = union.getResult(false, null);
    byte[] resultSketch = intermediateSketch.toByteArray();

    BytesWritable result = new BytesWritable();
    result.set(resultSketch, 0, resultSketch.length);

    return result;
  }
}
