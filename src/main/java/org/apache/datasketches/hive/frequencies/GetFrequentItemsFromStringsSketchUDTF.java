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

import java.util.Arrays;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "GetFrequentItems", value = "_FUNC_(sketch, errorType) - "
    + "Returns a list of frequent items in descending order by estimated frequency."
    + " Error type is optional and must be one of the following: "
    + "NO_FALSE_POSITIVES (default) or NO_FALSE_NEGATIVES.")
@SuppressWarnings("javadoc")
public class GetFrequentItemsFromStringsSketchUDTF extends GenericUDTF {

  PrimitiveObjectInspector inputObjectInspector;
  PrimitiveObjectInspector errorTypeObjectInspector;

  @SuppressWarnings("deprecation")
  @Override
  public StructObjectInspector initialize(final ObjectInspector[] inspectors) throws UDFArgumentException {
    if (inspectors.length != 1 && inspectors.length != 2) {
      throw new UDFArgumentException("One or two arguments expected");
    }

    if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Primitive argument expected, but "
          + inspectors[0].getCategory().name() + " was recieved");
    }
    inputObjectInspector = (PrimitiveObjectInspector) inspectors[0];
    if (inputObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
      throw new UDFArgumentTypeException(0, "Binary value expected as the first argument, but "
          + inputObjectInspector.getPrimitiveCategory().name() + " was recieved");
    }

    if (inspectors.length > 1) {
      if (inspectors[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(1, "Primitive argument expected, but "
            + inspectors[1].getCategory().name() + " was recieved");
      }
      errorTypeObjectInspector = (PrimitiveObjectInspector) inspectors[1];
      if (errorTypeObjectInspector.getPrimitiveCategory()
          != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        throw new UDFArgumentTypeException(1, "String value expected as the first argument, but "
            + errorTypeObjectInspector.getPrimitiveCategory().name() + " was recieved");
      }
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("item", "estimate", "lower_bound", "upper_bound"),
      Arrays.asList(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaLongObjectInspector,
          PrimitiveObjectInspectorFactory.javaLongObjectInspector,
          PrimitiveObjectInspectorFactory.javaLongObjectInspector
      )
    );
  }

  @Override
  public void process(final Object[] data) throws HiveException {
    if (data == null || data[0] == null) { return; }
    final BytesWritable serializedSketch =
        (BytesWritable) inputObjectInspector.getPrimitiveWritableObject(data[0]);
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
        BytesWritableHelper.wrapAsMemory(serializedSketch), new ArrayOfStringsSerDe());
    ErrorType errorType = ErrorType.NO_FALSE_POSITIVES;
    if (data.length > 1) {
      errorType = ErrorType.valueOf((String) errorTypeObjectInspector.getPrimitiveJavaObject(data[1]));
    }
    final ItemsSketch.Row<String>[] result = sketch.getFrequentItems(errorType);
    for (int i = 0; i < result.length; i++) {
      forward(new Object[] {
        result[i].getItem(), result[i].getEstimate(), result[i].getLowerBound(), result[i].getUpperBound()
      });
    }
  }

  @Override
  public void close() throws HiveException {
  }

}
