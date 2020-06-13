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

package org.apache.datasketches.hive.tuple;

import java.util.List;

import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class DoubleSummarySketchToEstimatesUDFTest {

  @Test
  public void nullSketch() {
    List<Double> result = new DoubleSummarySketchToEstimatesUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
    List<Double> result = new DoubleSummarySketchToEstimatesUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.0);
  }

  @Test
  public void normalCase() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
    sketch.update(1, 1.0);
    sketch.update(2, 1.0);
    List<Double> result = new DoubleSummarySketchToEstimatesUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 2.0);
    Assert.assertEquals(result.get(1), 2.0);
  }

}
