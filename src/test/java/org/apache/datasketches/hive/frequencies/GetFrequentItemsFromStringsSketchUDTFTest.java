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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;

@SuppressWarnings("javadoc")
public class GetFrequentItemsFromStringsSketchUDTFTest {

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  static final ObjectInspector stringInspector =
      PrimitiveObjectInspectorFactory.javaStringObjectInspector;

  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentException.class)
  public void initializeTooFewInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDTF func = new GetFrequentItemsFromStringsSketchUDTF();
    func.initialize(inspectors);
  }

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentException.class)
  public void initializeTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, stringInspector, stringInspector };
    GenericUDTF func = new GetFrequentItemsFromStringsSketchUDTF();
    func.initialize(inspectors);
  }

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
    Arrays.asList("a"),
    Arrays.asList(stringInspector)
  );

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentException.class)
  public void initializeWrongCategoryArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDTF func = new GetFrequentItemsFromStringsSketchUDTF();
    func.initialize(inspectors);
  }

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentException.class)
  public void initializeWrongCategoryArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, structInspector };
    GenericUDTF func = new GetFrequentItemsFromStringsSketchUDTF();
    func.initialize(inspectors);
  }

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentException.class)
  public void initializeWrongTypeArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, stringInspector };
    GenericUDTF func = new GetFrequentItemsFromStringsSketchUDTF();
    func.initialize(inspectors);
  }

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentException.class)
  public void initializeWrongTypeArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
    GenericUDTF func = new GetFrequentItemsFromStringsSketchUDTF();
    func.initialize(inspectors);
  }

  @Test
  public void normalCase() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, stringInspector };
    GenericUDTF func = new GetFrequentItemsFromStringsSketchUDTF();
    @SuppressWarnings("deprecation")
    ObjectInspector resultInspector = func.initialize(inspectors);
    checkResultInspector(resultInspector);
    ItemsSketch<String> sketch = new ItemsSketch<>(8);
    sketch.update("1", 10);
    sketch.update("2");
    sketch.update("3");
    sketch.update("4");
    sketch.update("5");
    sketch.update("6");
    sketch.update("7", 15);
    sketch.update("8");
    sketch.update("9");
    sketch.update("10");
    sketch.update("11");
    sketch.update("12");

    {
      MockCollector collector = new MockCollector();
      func.setCollector(collector);
      func.process(new Object[] { new BytesWritable(sketch.toByteArray(serDe)) });
      Assert.assertEquals(collector.list.size(), 2);
    }
    {
      MockCollector collector = new MockCollector();
      func.setCollector(collector);
      func.process(new Object[] { new BytesWritable(sketch.toByteArray(serDe)), "NO_FALSE_NEGATIVES" });
      Assert.assertTrue(collector.list.size() >= 2);
    }
  }

  private static void checkResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.STRUCT);
    List<? extends StructField> fields = ((StructObjectInspector) resultInspector).getAllStructFieldRefs();
    Assert.assertEquals(fields.size(), 4);
    Assert.assertEquals(fields.get(0).getFieldObjectInspector().getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) fields.get(0).getFieldObjectInspector()).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.STRING
    );
    Assert.assertEquals(fields.get(1).getFieldObjectInspector().getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) fields.get(1).getFieldObjectInspector()).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.LONG
    );
    Assert.assertEquals(fields.get(2).getFieldObjectInspector().getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) fields.get(2).getFieldObjectInspector()).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.LONG
    );
    Assert.assertEquals(fields.get(3).getFieldObjectInspector().getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) fields.get(3).getFieldObjectInspector()).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.LONG
    );
  }

  private static class MockCollector implements Collector {
    List<Object> list = new ArrayList<>();

    @Override
    public void collect(Object object) throws HiveException {
      list.add(object);
    }

  }

}
