/*
 * Copyright 2015 by the Metanome project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.metanome.algorithm_helper.data_structures;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class PLIBuilderFixture {

  protected ArrayList<ArrayList<String>> table = new ArrayList<>();
  protected RelationalInput input;
  int rowPosition;

  public PLIBuilderFixture() {
    table.add(new ArrayList<>(Arrays.asList("1", "1", "5", null)));
    table.add(new ArrayList<>(Arrays.asList("2", "1", "5", "2")));
    table.add(new ArrayList<>(Arrays.asList("3", "1", "3", null)));
    table.add(new ArrayList<>(Arrays.asList("4", "1", "3", "4")));
    table.add(new ArrayList<>(Arrays.asList("5", "1", "5", "5")));
  }

  public RelationalInputGenerator getInputGenerator()
    throws InputGenerationException, InputIterationException
  {
    RelationalInputGenerator inputGenerator = mock(RelationalInputGenerator.class);
    this.input = this.getRelationalInput();
    when(inputGenerator.generateNewCopy())
      .thenAnswer(new Answer<RelationalInput>() {
        public RelationalInput answer(InvocationOnMock invocation) throws Throwable {
          rowPosition = 0;
          return input;
        }
      });
    return inputGenerator;
  }

  protected RelationalInput getRelationalInput() throws InputIterationException {
    RelationalInput input = mock(RelationalInput.class);

    when(input.hasNext()).thenAnswer(new Answer<Boolean>() {
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return rowPosition < table.size();
      }
    });

    when(input.next()).thenAnswer(new Answer<ArrayList<String>>() {
      public ArrayList<String> answer(InvocationOnMock invocation) throws Throwable {
        rowPosition += 1;
        return table.get(rowPosition - 1);
      }
    });

    when(input.numberOfColumns()).thenReturn(4);

    return input;
  }


  public int getExpectedNumberOfTuples() {
    return table.size();
  }

  public List<TreeSet<String>> getExpectedDistinctSortedColumns() {
    List<TreeSet<String>> distinctSortedColumns = new LinkedList<>();

    for (int col = 0; col < table.get(0).size(); col++) {
      TreeSet<String> sortedcolumn = new TreeSet<>();
      for (ArrayList<String> row : table) {
        String value = row.get(col);
        if (value != null) {
          sortedcolumn.add(value);
        }
      }
      distinctSortedColumns.add(sortedcolumn);
    }

    return distinctSortedColumns;
  }

  public List<PositionListIndex> getExpectedPLIList(boolean nullEqualsNull) {
    List<PositionListIndex> expectedPLIList = new LinkedList<>();
    List<IntArrayList> list1 = new LinkedList<>();
    PositionListIndex PLI1 = new PositionListIndex(list1);
    expectedPLIList.add(PLI1);

    List<IntArrayList> list2 = new LinkedList<>();
    IntArrayList arrayList21 = new IntArrayList();
    arrayList21.add(0);
    arrayList21.add(1);
    arrayList21.add(2);
    arrayList21.add(3);
    arrayList21.add(4);
    list2.add(arrayList21);
    PositionListIndex PLI2 = new PositionListIndex(list2);
    expectedPLIList.add(PLI2);

    List<IntArrayList> list3 = new LinkedList<>();
    IntArrayList arrayList31 = new IntArrayList();
    IntArrayList arrayList32 = new IntArrayList();

    arrayList31.add(0);
    arrayList31.add(1);
    arrayList31.add(4);

    arrayList32.add(2);
    arrayList32.add(3);

    list3.add(arrayList31);
    list3.add(arrayList32);
    PositionListIndex PLI3 = new PositionListIndex(list3);
    expectedPLIList.add(PLI3);

    List<IntArrayList> list4 = new LinkedList<>();
    if (nullEqualsNull) {
      IntArrayList arrayList41 = new IntArrayList();

      arrayList41.add(0);
      arrayList41.add(2);

      list4.add(arrayList41);
    }

    PositionListIndex PLI4 = new PositionListIndex(list4);
    expectedPLIList.add(PLI4);
    return expectedPLIList;
  }
}
