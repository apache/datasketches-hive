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

package org.apache.datasketches.hive.cpc;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.cpc.CpcSketch;

@SuppressWarnings("javadoc")
public class UnionSketchUDFTest {

  @Test
  public void nullInputs() {
    UnionSketchUDF udf = new UnionSketchUDF();
    BytesWritable result = udf.evaluate(null, null);
    CpcSketch resultSketch = CpcSketch.heapify(BytesWritableHelper.wrapAsMemory(result));
    Assert.assertTrue(resultSketch.isEmpty());
    Assert.assertEquals(resultSketch.getEstimate(), 0.0);
  }

  @Test
  public void validSketches() {
    UnionSketchUDF udf = new UnionSketchUDF();

    CpcSketch sketch1 = new CpcSketch(SketchEvaluator.DEFAULT_LG_K);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    CpcSketch sketch2 = new CpcSketch(SketchEvaluator.DEFAULT_LG_K);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toByteArray());

    BytesWritable result = udf.evaluate(input1, input2);

    CpcSketch resultSketch = CpcSketch.heapify(BytesWritableHelper.wrapAsMemory(result));

    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.02);
  }

  @Test
  public void validSketchesExplicitLgK () {
    UnionSketchUDF udf = new UnionSketchUDF();

    final int lgK = 10;

    CpcSketch sketch1 = new CpcSketch(lgK);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    CpcSketch sketch2 = new CpcSketch(lgK);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toByteArray());

    BytesWritable result = udf.evaluate(input1, input2, lgK);

    CpcSketch resultSketch = CpcSketch.heapify(BytesWritableHelper.wrapAsMemory(result));

    Assert.assertEquals(resultSketch.getLgK(), lgK);
    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.02);
  }

  @Test
  public void validSketchesExplicitLgKAndSeed () {
    UnionSketchUDF udf = new UnionSketchUDF();

    final int lgK = 10;
    final long seed = 123;

    CpcSketch sketch1 = new CpcSketch(lgK, seed);
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    CpcSketch sketch2 = new CpcSketch(lgK, seed);
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.toByteArray());

    BytesWritable result = udf.evaluate(input1, input2, lgK, seed);

    CpcSketch resultSketch = CpcSketch.heapify(BytesWritableHelper.wrapAsMemory(result), seed);

    Assert.assertEquals(resultSketch.getLgK(), lgK);
    Assert.assertEquals(resultSketch.getEstimate(), 256.0, 256 * 0.02);
  }

}
