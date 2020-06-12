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

package org.apache.datasketches.hive.hll;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

@SuppressWarnings("javadoc")
public class UnionSketchUDFTest {

  @Test
  public void nullInputs() {
    UnionSketchUDF udf = new UnionSketchUDF();
    BytesWritable result = udf.evaluate(null, null);
    HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory(result));
    Assert.assertTrue(resultSketch.isEmpty());
    Assert.assertEquals(resultSketch.getEstimate(), 0.0);
  }

  @Test
  public void validSketches() {
    UnionSketchUDF udf = new UnionSketchUDF();

    HllSketch sketch1 = new HllSketch(SketchEvaluator.DEFAULT_LG_K);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    HllSketch sketch2 = new HllSketch(SketchEvaluator.DEFAULT_LG_K);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toCompactByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toCompactByteArray());

    BytesWritable result = udf.evaluate(input1, input2);

    HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory(result));

    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.01);
  }

  @Test
  public void validSketchesExplicitParams () {
    UnionSketchUDF udf = new UnionSketchUDF();

    final int lgK = 10;
    final TgtHllType type = TgtHllType.HLL_6;

    HllSketch sketch1 = new HllSketch(10);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    HllSketch sketch2 = new HllSketch(10);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toCompactByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toCompactByteArray());

    BytesWritable result = udf.evaluate(input1, input2, lgK, type.toString());

    HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory(result));

    Assert.assertEquals(resultSketch.getLgConfigK(), lgK);
    Assert.assertEquals(resultSketch.getTgtHllType(), type);
    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.02);
  }

}
