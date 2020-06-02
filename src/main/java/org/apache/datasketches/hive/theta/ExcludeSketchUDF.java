/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.theta;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.theta.AnotB;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

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
      firstSketch = Sketch.wrap(BytesWritableHelper.wrapAsMemory(firstSketchBytes), hashSeed);
    }

    Sketch secondSketch = null;
    if (secondSketchBytes != null && secondSketchBytes.getLength() > 0) {
      secondSketch = Sketch.wrap(BytesWritableHelper.wrapAsMemory(secondSketchBytes), hashSeed);
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
