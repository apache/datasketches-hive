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

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;

@SuppressWarnings("javadoc")
public class UnionSketchUDFTest {

  @Test
  public void evaluateNull() {
    UnionSketchUDF testObject = new UnionSketchUDF();
    BytesWritable intermResult = testObject.evaluate(null, null);
    Memory mem = BytesWritableHelper.wrapAsMemory(intermResult);
    Sketch testResult = Sketches.wrapSketch(mem);
    Assert.assertEquals(testResult.getEstimate(), 0.0);
  }

  @Test
  public void testEvaluateEmpty() {
    UnionSketchUDF testObject = new UnionSketchUDF();
    BytesWritable intermResult = testObject.evaluate(new BytesWritable(), new BytesWritable());
    Memory mem = BytesWritableHelper.wrapAsMemory(intermResult);
    Sketch testResult = Sketches.wrapSketch(mem);
    Assert.assertEquals(testResult.getEstimate(), 0.0);
  }

  @Test
  public void evaluateValidSketch () {
    UnionSketchUDF testObject = new UnionSketchUDF();

    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact().toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact().toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2);

    Sketch result = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory(output));

    Assert.assertEquals(result.getEstimate(), 256.0);
  }

  @Test
  public void evaluateValidSketchExplicitSizeAndSeed () {
    UnionSketchUDF testObject = new UnionSketchUDF();

    final long seed = 1;
    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setSeed(seed).setNominalEntries(1024).build();
    for (int i = 0; i < 128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setSeed(seed).setNominalEntries(1024).build();
    for (int i = 100; i < 256; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact().toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact().toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2, 128, seed);

    Sketch result = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory(output), seed);

    Assert.assertEquals(result.getEstimate(), 256.0, 256 * 0.02);
    Assert.assertTrue(result.getRetainedEntries(true) <= 128.0);
  }

}
