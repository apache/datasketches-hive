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
import static org.testng.AssertJUnit.assertEquals;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.io.BytesWritable;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;

@SuppressWarnings("javadoc")
public class ExcludeSketchUDFTest {

  @Test
  public void evaluateNull() {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    BytesWritable intermResult = testObject.evaluate(null, null);

    Memory mem = BytesWritableHelper.wrapAsMemory(intermResult);

    Sketch testResult = Sketches.wrapSketch(mem);

    assertEquals(0.0, testResult.getEstimate());
  }

  @Test
  public void evaluateEmpty() {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    BytesWritable intermResult = testObject.evaluate(new BytesWritable(), new BytesWritable());

    Memory mem = BytesWritableHelper.wrapAsMemory(intermResult);

    Sketch testResult = Sketches.wrapSketch(mem);

    assertEquals(0.0, testResult.getEstimate());
  }

  @Test
  public void evaluateValidSketch () {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact(true, null).toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact(true, null).toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2);

    Sketch result = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory(output));

    assertEquals(100.0, result.getEstimate());
  }

  @Test
  public void evaluateValidSketchWithDefaultSeed () {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact(true, null).toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact(true, null).toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2, DEFAULT_UPDATE_SEED);

    Sketch result = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory(output));

    assertEquals(100.0, result.getEstimate());
  }

  @Test
  public void evaluateValidSketchWithCustomSeed () {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    final long seed = 1;
    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setSeed(seed).setNominalEntries(1024).build();
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setSeed(seed).setNominalEntries(1024).build();
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact(true, null).toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact(true, null).toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2, seed);

    Sketch result = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory(output), seed);

    assertEquals(100.0, result.getEstimate());
  }

}
