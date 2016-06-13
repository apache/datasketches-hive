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
   * @param firstSketchBytes
   *          first sketch to be intersected.
   * @param secondSketchBytes
   *          second sketch to be intersected.
   * @param hashUpdateSeed
   *          Only required if input sketches were constructed using an update seed that was not the default.
   * @return resulting sketch of intersection.
   */
  public BytesWritable evaluate(final BytesWritable firstSketchBytes, final BytesWritable secondSketchBytes, final LongWritable hashUpdateSeed) {
    final long hashSeed = (hashUpdateSeed == null)? DEFAULT_UPDATE_SEED : hashUpdateSeed.get();

    Sketch firstSketch = null;
    if (firstSketchBytes != null && firstSketchBytes.getLength() > 0) {
      firstSketch = Sketch.wrap(new NativeMemory(firstSketchBytes.getBytes()));
    }

    Sketch secondSketch = null;
    if (secondSketchBytes != null && secondSketchBytes.getLength() > 0) {
      secondSketch = Sketch.wrap(new NativeMemory(secondSketchBytes.getBytes()));
    }

    final Intersection intersect = SetOperation.builder().setSeed(hashSeed).buildIntersection();
    intersect.update(firstSketch);
    intersect.update(secondSketch);
    final byte[] resultSketchBytes = intersect.getResult(true, null).toByteArray();
    final BytesWritable result = new BytesWritable();
    result.set(resultSketchBytes, 0, resultSketchBytes.length);
    return result;
  }

  /**
   * Main logic called by hive if sketchSize is not passed in. Computes the
   * intersection of two sketches of same or different column.
   * 
   * @param firstSketchBytes
   *          first sketch to be intersected.
   * @param secondSketchBytes
   *          second sketch to be intersected.
   * @return resulting sketch of intersection.
   */
  public BytesWritable evaluate(final BytesWritable firstSketchBytes, final BytesWritable secondSketchBytes) {
    return evaluate(firstSketchBytes, secondSketchBytes, null);
  }
}
