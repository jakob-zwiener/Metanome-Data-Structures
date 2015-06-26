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

import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * Constructs a list of {@link PositionListIndex}es from the given {@link
 * de.metanome.algorithm_integration.input.RelationalInput}. A list of all columns' sorted distinct
 * values can be constructed as a byproduct.
 */
public class PLIBuilder implements GenericPLIBuilder {

  protected int numberOfTuples = -1;
  protected List<HashMap<String, IntArrayList>> columns = null;
  protected RelationalInput input;
  protected boolean nullEqualsNull;

  public PLIBuilder(RelationalInput input) {
    this.input = input;
    this.nullEqualsNull = true;
  }

  public PLIBuilder(RelationalInput input, boolean nullEqualsNull) {
    this(input);
    this.nullEqualsNull = nullEqualsNull;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<PositionListIndex> getPLIList() throws PLIBuildingException {
    List<List<IntArrayList>> rawPLIs;
    try {
      rawPLIs = getRawPLIs();
    }
    catch (InputIterationException e) {
      throw new PLIBuildingException(
        "The pli could not be built, because there was an error iterating over the input.", e);
    }
    List<PositionListIndex> result = new ArrayList<>();
    for (List<IntArrayList> rawPLI : rawPLIs) {
      result.add(new PositionListIndex(rawPLI));
    }
    return result;
  }

  /**
   * Calculates the raw PositionListIndices
   * @return list of associated clusters (PLI)
   * @throws InputIterationException if the input cannot be iterated
   */
  protected List<List<IntArrayList>> getRawPLIs() throws InputIterationException {
    if (columns == null) {
      columns = new ArrayList<>();
      calculateUnpurgedPLI();
    }
    return purgePLIEntries();
  }

  /**
   * Returns the number of tuples in the input after calculating the plis. Can be used after
   * calculateUnpurgedPLI was called.
   * @return number of tuples in dataset
   */
  public int getNumberOfTuples() throws InputIterationException {
    if (this.numberOfTuples == -1) {
      throw new InputIterationException();
    }
    else {
      return this.numberOfTuples;
    }
  }

  /**
   * Builds a {@link TreeSet} of the values of every column in the input. "null" values are filtered
   * as they are not required for spider.
   * @return all comlumns' sorted distinct values
   * @throws InputIterationException if the input cannot be iterated
   */
  public List<TreeSet<String>> getDistinctSortedColumns() throws InputIterationException {
    if (columns == null) {
      columns = new ArrayList<>();
      calculateUnpurgedPLI();
    }

    List<TreeSet<String>> distinctSortedColumns = new LinkedList<>();

    for (HashMap<String, IntArrayList> columnMap : columns) {
      if (columnMap.containsKey(null)) {
        columnMap.remove(null);
      }
      distinctSortedColumns.add(new TreeSet<>(columnMap.keySet()));
    }

    return distinctSortedColumns;
  }

  protected void calculateUnpurgedPLI() throws InputIterationException {
    int rowCount = 0;
    this.numberOfTuples = 0;
    while (input.hasNext()) {
      this.numberOfTuples++;
      List<String> row = input.next();
      int columnCount = 0;
      for (String cellValue : row) {
        addValue(rowCount, columnCount, cellValue);
        columnCount++;
      }
      rowCount++;
    }
  }

  protected void addValue(int rowCount, int columnCount, String attributeCell) {
    if (columns.size() <= columnCount) {
      columns.add(new HashMap<String, IntArrayList>());
    }

    if (!this.nullEqualsNull && attributeCell == null) {
      return;
    }

    if (columns.get(columnCount).containsKey(attributeCell)) {
      columns.get(columnCount).get(attributeCell).add(rowCount);
    }
    else {
      IntArrayList newList = new IntArrayList();
      newList.add(rowCount);
      columns.get(columnCount).put(attributeCell, newList);
    }
  }

  protected List<List<IntArrayList>> purgePLIEntries() {
    List<List<IntArrayList>> rawPLIList = new ArrayList<>();
    Iterator<HashMap<String, IntArrayList>> columnsIterator = columns.iterator();
    while (columnsIterator.hasNext()) {
      List<IntArrayList> clusters = new ArrayList<>();
      for (IntArrayList cluster : columnsIterator.next().values()) {
        if (cluster.size() < 2) {
          continue;
        }
        clusters.add(cluster);
      }
      rawPLIList.add(clusters);
      // Free value Maps.
      columnsIterator.remove();
    }
    return rawPLIList;
  }
}
