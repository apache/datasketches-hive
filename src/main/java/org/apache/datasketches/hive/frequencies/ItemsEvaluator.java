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

package org.apache.datasketches.hive.frequencies;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.memory.Memory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

abstract class ItemsEvaluator<T> extends GenericUDAFEvaluator {

  private final ArrayOfItemsSerDe<T> serDe_;
  protected PrimitiveObjectInspector inputObjectInspector;

  ItemsEvaluator(final ArrayOfItemsSerDe<T> serDe) {
    serDe_ = serDe;
  }

  @Override
  public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
    super.init(mode, parameters);
    inputObjectInspector = (PrimitiveObjectInspector) parameters[0];
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void reset(final AggregationBuffer buf) throws HiveException {
    @SuppressWarnings("unchecked")
    final ItemsState<T> state = (ItemsState<T>) buf;
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
    @SuppressWarnings("unchecked")
    final ItemsState<T> state = (ItemsState<T>) buf;
    final Memory serializedSketch = BytesWritableHelper.wrapAsMemory(
        (BytesWritable) inputObjectInspector.getPrimitiveWritableObject(data));
    state.update(serializedSketch);
  }

  @SuppressWarnings("deprecation")
  @Override
  public Object terminate(final AggregationBuffer buf) throws HiveException {
    @SuppressWarnings("unchecked")
    final ItemsState<T> state = (ItemsState<T>) buf;
    final ItemsSketch<T> resultSketch = state.getResult();
    if (resultSketch == null) { return null; }
    return new BytesWritable(resultSketch.toByteArray(serDe_));
  }

  @SuppressWarnings("deprecation")
  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new ItemsState<>(serDe_);
  }

}
