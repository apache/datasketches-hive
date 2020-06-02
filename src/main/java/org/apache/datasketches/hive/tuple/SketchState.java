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

import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SummaryFactory;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.UpdatableSummary;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

class SketchState<U, S extends UpdatableSummary<U>> extends State<S> {

  private UpdatableSketch<U, S> sketch_;

  boolean isInitialized() {
    return sketch_ != null;
  }

  void init(final int nominalNumEntries, final float samplingProbability,
      final SummaryFactory<S> summaryFactory) {
    super.init(nominalNumEntries);
    sketch_ = new UpdatableSketchBuilder<U, S>(summaryFactory).setNominalEntries(nominalNumEntries)
        .setSamplingProbability(samplingProbability).build();
  }

  void update(final Object data, final PrimitiveObjectInspector keyObjectInspector, final U value) {
    switch (keyObjectInspector.getPrimitiveCategory()) {
    case BINARY:
      sketch_.update(PrimitiveObjectInspectorUtils.getBinary(data, keyObjectInspector).copyBytes(), value);
      return;
    case BYTE:
      sketch_.update(PrimitiveObjectInspectorUtils.getByte(data, keyObjectInspector), value);
      return;
    case DOUBLE:
      sketch_.update(PrimitiveObjectInspectorUtils.getDouble(data, keyObjectInspector), value);
      return;
    case FLOAT:
      sketch_.update(PrimitiveObjectInspectorUtils.getFloat(data, keyObjectInspector), value);
      return;
    case INT:
      sketch_.update(PrimitiveObjectInspectorUtils.getInt(data, keyObjectInspector), value);
      return;
    case LONG:
      sketch_.update(PrimitiveObjectInspectorUtils.getLong(data, keyObjectInspector), value);
      return;
    case STRING:
      sketch_.update(PrimitiveObjectInspectorUtils.getString(data, keyObjectInspector), value);
      return;
    default:
      throw new IllegalArgumentException(
          "Unrecongnized input data type, please use data of type: "
      + "byte, double, float, int, long, or string only.");
    }
  }

  @Override
  Sketch<S> getResult() {
    if (sketch_ == null) { return null; }
    // assumes that it is called once at the end of processing
    // since trimming to nominal number of entries is expensive
    sketch_.trim();
    return sketch_.compact();
  }

  @Override
  void reset() {
    sketch_ = null;
  }

}
