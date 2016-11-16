/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.theta.AnotB;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;

/**
 * Hive exclude sketch UDF. (i.e. in sketch a but not in sketch b)
 *
 */
public class ExcludeSketchUDF extends UDF {

  /**
   * Main logic called by hive if sketchSize is also passed in. Computes the
   * hash in first sketch excluding the hash in second sketch of two sketches of
   * same or different column.
   *
   * @param firstSketchBytes
   *          first sketch to be included.
   * @param secondSketchBytes
   *          second sketch to be excluded.
   * @param hashSeed
   *          Only required if input sketches were constructed using an update seed that was not the default.
   * @return resulting sketch of exclusion.
   */
  public BytesWritable evaluate(final BytesWritable firstSketchBytes,
      final BytesWritable secondSketchBytes, final long hashSeed) {

    Sketch firstSketch = null;
    if (firstSketchBytes != null && firstSketchBytes.getLength() > 0) {
      firstSketch = Sketch.wrap(new NativeMemory(firstSketchBytes.getBytes()), hashSeed);
    }

    Sketch secondSketch = null;
    if (secondSketchBytes != null && secondSketchBytes.getLength() > 0) {
      secondSketch = Sketch.wrap(new NativeMemory(secondSketchBytes.getBytes()), hashSeed);
    }

    final AnotB anotb = SetOperation.builder().setSeed(hashSeed).buildANotB();
    anotb.update(firstSketch, secondSketch);
    final byte[] excludeSketchBytes = anotb.getResult().toByteArray();
    final BytesWritable result = new BytesWritable();
    result.set(excludeSketchBytes, 0, excludeSketchBytes.length);
    return result;
  }

  /**
   * Main logic called by hive if hashUpdateSeed is not passed in. Computes the hash
   * in first sketch excluding the hash in second sketch of two sketches of same
   * or different column.
   *
   * @param firstSketchBytes
   *          first sketch to be included.
   * @param secondSketchBytes
   *          second sketch to be excluded.
   * @return resulting sketch of exclusion.
   */
  public BytesWritable evaluate(final BytesWritable firstSketchBytes, final BytesWritable secondSketchBytes) {
    return evaluate(firstSketchBytes, secondSketchBytes, DEFAULT_UPDATE_SEED);
  }

}
