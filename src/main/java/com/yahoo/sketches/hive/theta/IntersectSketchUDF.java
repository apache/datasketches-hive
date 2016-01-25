/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;

/**
 * Hive intersection sketch UDF.
 *
 */
public class IntersectSketchUDF extends UDF {

  /**
   * Main logic called by hive if sketchSize is also passed in. Computes the
   * intersection of two sketches of same or different column.
   * 
   * @param firstSketch
   *          first sketch to be intersected.
   * @param secondSketch
   *          second sketch to be intersected.
   * @param hashUpdateSeed
   *          Only required if input sketches were constructed using an update seed that was not the default.
   * @return resulting sketch of intersection.
   */
  public BytesWritable evaluate(BytesWritable firstSketch, BytesWritable secondSketch, LongWritable hashUpdateSeed) {
    long hashSeed = (hashUpdateSeed == null)? DEFAULT_UPDATE_SEED : hashUpdateSeed.get();
    
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

    Intersection intersect = SetOperation.builder().setSeed(hashSeed).buildIntersection();
    
    intersect.update(firstHeapSketch);
    intersect.update(secondHeapSketch);

    Sketch intermediateSketch = intersect.getResult(true, null);
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
