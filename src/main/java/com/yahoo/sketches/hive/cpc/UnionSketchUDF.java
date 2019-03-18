/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.cpc.CpcSketch;
import com.yahoo.sketches.cpc.CpcUnion;

/**
 * Hive union sketch UDF.
 */
public class UnionSketchUDF extends UDF {

  /**
   * Union two sketches given explicit lgK and seed
   *
   * @param firstSketch
   *   first sketch to be unioned.
   * @param secondSketch
   *   second sketch to be unioned.
   * @param lgK
   *   final output lgK
   *   This must be between 4 and 26.
   * @param seed
   *   update seed
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch,
      final int lgK, final long seed) {

    final CpcUnion union = new CpcUnion(lgK, seed);

    if (firstSketch != null) {
      union.update(CpcSketch.heapify(Memory.wrap(firstSketch.getBytes()), seed));
    }

    if (secondSketch != null) {
      union.update(CpcSketch.heapify(Memory.wrap(secondSketch.getBytes()), seed));
    }

    return new BytesWritable(union.getResult().toByteArray());
  }

  /**
   * Union two sketches given explicit lgK and using default seed
   *
   * @param firstSketch
   *   first sketch to be unioned.
   * @param secondSketch
   *   second sketch to be unioned.
   * @param lgK
   *   final output lgK
   *   This must be between 4 and 26.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch,
      final int lgK) {
    return evaluate(firstSketch, secondSketch, lgK, DEFAULT_UPDATE_SEED);
  }

  /**
   * Union two sketches using default lgK an seed
   *
   * @param firstSketch
   *    first sketch to be unioned.
   * @param secondSketch
   *    second sketch to be unioned.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch) {
    return evaluate(firstSketch, secondSketch, SketchEvaluator.DEFAULT_LG_K, DEFAULT_UPDATE_SEED);
  }

}
