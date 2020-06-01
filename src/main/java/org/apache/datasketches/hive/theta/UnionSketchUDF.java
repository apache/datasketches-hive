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

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

/**
 * Hive union sketch UDF.
 */
public class UnionSketchUDF extends UDF {

  private static final int EMPTY_SKETCH_SIZE_BYTES = 8;

  /**
   * Main logic called by hive if sketchSize is also passed in. Union two
   * sketches of same or different column.
   *
   * @param firstSketch
   *          first sketch to be unioned.
   * @param secondSketch
   *          second sketch to be unioned.
   * @param sketchSize
   *          final output unioned sketch size.
   *          This must be a power of 2 and larger than 16.
   * @param seed using the seed is not recommended unless you really know why you need it.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch,
      final int sketchSize, final long seed) {

    final Union union = SetOperation.builder().setSeed(seed).setNominalEntries(sketchSize).buildUnion();

    if ((firstSketch != null) && (firstSketch.getLength() >= EMPTY_SKETCH_SIZE_BYTES)) {
      union.update(BytesWritableHelper.wrapAsMemory(firstSketch));
    }

    if ((secondSketch != null) && (secondSketch.getLength() >= EMPTY_SKETCH_SIZE_BYTES)) {
      union.update(BytesWritableHelper.wrapAsMemory(secondSketch));
    }

    return new BytesWritable(union.getResult().toByteArray());
  }

  /**
   * Main logic called by hive if sketchSize is also passed in. Union two
   * sketches of same or different column.
   *
   * @param firstSketch
   *          first sketch to be unioned.
   * @param secondSketch
   *          second sketch to be unioned.
   * @param sketchSize
   *          final output unioned sketch size.
   *          This must be a power of 2 and larger than 16.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch,
      final int sketchSize) {
    return evaluate(firstSketch, secondSketch, sketchSize, DEFAULT_UPDATE_SEED);
  }

  /**
   * Main logic called by hive if sketchSize is not passed in. Union two
   * sketches of same or different column.
   *
   * @param firstSketch
   *          first sketch to be unioned.
   * @param secondSketch
   *          second sketch to be unioned.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch) {
    return evaluate(firstSketch, secondSketch, DEFAULT_NOMINAL_ENTRIES, DEFAULT_UPDATE_SEED);
  }

}
