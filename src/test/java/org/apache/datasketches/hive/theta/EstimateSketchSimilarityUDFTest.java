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

import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.hadoop.io.BytesWritable;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@SuppressWarnings("javadoc")
public class EstimateSketchSimilarityUDFTest {

  @Test
  public void evaluateNull() {
    EstimateSketchSimilarityUDF testObject = new EstimateSketchSimilarityUDF();
    double testResult = testObject.evaluate(null, null);
    assertEquals(0.0, testResult);
  }

  @Test
  public void evaluateEmpty() {
    EstimateSketchSimilarityUDF testObject = new EstimateSketchSimilarityUDF();
    double testResult = testObject.evaluate(new BytesWritable(), new BytesWritable());
    assertEquals(0.0, testResult);
  }

  @Test
  public void evaluateValidSketch() {
    EstimateSketchSimilarityUDF testObject = new EstimateSketchSimilarityUDF();

    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact().toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact().toByteArray());

    double result = testObject.evaluate(input1, input2);

    assertEquals(28.0 / 128.0, result);
  }

}
