/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.yahoo.sketches.theta.SetOpReturnState;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;

/**
 * Hive intersection sketch UDF.
 *
 */
public class IntersectSketchUDF extends UDF {
  public static final int DEFAULT_SIZE = 16384;

  /**
   * Main logic called by hive if sketchSize is also passed in. Computes the
   * intersection of two sketches of same or different column.
   * 
   * @param firstSketch
   *          first sketch to be intersected.
   * @param secondSketch
   *          second sketch to be intersected.
   * @param sketchSize
   *          final output intersected sketch size.
   * @return resulting sketch of intersection.
   */
  public BytesWritable evaluate(BytesWritable firstSketch, BytesWritable secondSketch, IntWritable sketchSize) {
    int sketch_size = DEFAULT_SIZE;
    
    Sketch firstHeapSketch, secondHeapSketch;
    
    // if first sketch is null, throw exception
    if (firstSketch == null || firstSketch.getLength() == 0) {
      firstHeapSketch = null;
    } else {
      NativeMemory firstMemory = new NativeMemory(firstSketch.getBytes());
      firstHeapSketch = Sketch.heapify(firstMemory);
    }

    // if second sketch is null, continue with second sketch as empty sketch
    if (secondSketch == null || secondSketch.getLength() == 0) {
      secondHeapSketch = null;
    } else {
      NativeMemory secondMemory = new NativeMemory(secondSketch.getBytes());
      secondHeapSketch = Sketch.heapify(secondMemory);
    }

    if (sketchSize != null) {
      sketch_size = sketchSize.get();
    }

    Intersection intersect = (Intersection) SetOperation.builder().build(sketch_size, Family.INTERSECTION);
    
    SetOpReturnState success = intersect.update(firstHeapSketch);
    if (success != SetOpReturnState.Success) {
      throw new IllegalStateException("HiveSketchError: Initialize intersect first sketch operation failed.");
    }
    
    success = intersect.update(secondHeapSketch);
    if (success != SetOpReturnState.Success) {
      throw new IllegalStateException("HiveSketchError: intersect sketch operation failed.");
    }

    Sketch intermediateSketch = intersect.getResult(false, null);
    byte[] resultSketch = intermediateSketch.toByteArray();

    BytesWritable result = new BytesWritable();
    result.set(resultSketch, 0, resultSketch.length);

    return result;
  }

  /**
   * Main logic called by hive if sketchSize is not passed in. Computes the
   * intersection of two sketches of same or different column.
   * 
   * @param firstSketch
   *          first sketch to be intersected.
   * @param secondSketch
   *          second sketch to be intersected.
   * @return resulting sketch of intersection.
   */
  public BytesWritable evaluate(BytesWritable firstSketch, BytesWritable secondSketch) {

    return evaluate(firstSketch, secondSketch, null);
  }
}
