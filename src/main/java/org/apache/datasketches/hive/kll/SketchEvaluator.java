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

package org.apache.datasketches.hive.kll;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

abstract class SketchEvaluator extends GenericUDAFEvaluator {

  protected PrimitiveObjectInspector inputInspector_;
  protected PrimitiveObjectInspector kInspector_;

  @Override
  public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
    super.init(mode, parameters);
    inputInspector_ = (PrimitiveObjectInspector) parameters[0];

    // Parameters:
    // In PARTIAL1 and COMPLETE mode, the parameters are original data.
    // In PARTIAL2 and FINAL mode, the parameters are partial aggregations.
    if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
      if (parameters.length > 1) {
        kInspector_ = (PrimitiveObjectInspector) parameters[1];
      }
    }

    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void reset(final AggregationBuffer buf) throws HiveException {
    final SketchState state = (SketchState) buf;
    state.reset();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Object terminatePartial(final AggregationBuffer buf) throws HiveException {
    return terminate(buf);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void merge(final AggregationBuffer buf, final Object data) throws HiveException {
    if (data == null) { return; }
    final SketchState state = (SketchState) buf;
    final Memory serializedSketch = BytesWritableHelper.wrapAsMemory(
        (BytesWritable) inputInspector_.getPrimitiveWritableObject(data));
    state.update(KllFloatsSketch.heapify(serializedSketch));
  }

  @SuppressWarnings("deprecation")
  @Override
  public Object terminate(final AggregationBuffer buf) throws HiveException {
    final SketchState state = (SketchState) buf;
    final KllFloatsSketch resultSketch = state.getResult();
    if (resultSketch == null) { return null; }
    return new BytesWritable(resultSketch.toByteArray());
  }

  @SuppressWarnings("deprecation")
  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new SketchState();
  }

}
