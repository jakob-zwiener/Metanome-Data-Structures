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

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

/**
 * A graph that allows efficient lookup of all subsets in the graph for a given
 * ColumnCombinationBitset.
 *
 * @author Jens Ehrlich
 * @author Jakob Zwiener
 *
 * TODO(zwiener): This class should be renamed as it also allows superset lookups.
 */
public class SubSetGraph {

  protected SubSetGraph[] subGraphs;
  protected boolean subSetEnds = false;

  /**
   * Creates a new {@link SubSetGraph} that is able to store sets with the given alphabet size.
   *
   * @param dimension the size of the alphabet of this graph
   */
  public SubSetGraph(int dimension) {
    subGraphs = new SubSetGraph[dimension];
  }

  /**
   * Adds a column combination to the graph. Returns the graph after adding.
   * @param columnCombination a column combination to add
   * @return the graph
   * @throws ColumnIndexOutOfBoundsException when the columnCombination contains at least one column index that is larger or equal to the size of the alphabet
   */
  public SubSetGraph add(ColumnCombinationBitset columnCombination)
      throws ColumnIndexOutOfBoundsException {
    SubSetGraph subGraph = this;

    for (int setColumnIndex : columnCombination.getSetBits()) {
      if (setColumnIndex >= subGraphs.length) {
        throw new ColumnIndexOutOfBoundsException(
            String.format("Column index %d is larger than the size of the alphabet %d.",
                          setColumnIndex, subGraphs.length));
      }
      subGraph = subGraph.lazySubGraphGeneration(setColumnIndex);
    }
    subGraph.subSetEnds = true;
    return this;
  }

  /**
   * Adds all columnCombinations in the {@link java.util.Collection} to the graph.
   * @param columnCombinations a {@link java.util.Collection} of {@link de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset}s
   * to add to the graph
   * @return the graph
   */
  public SubSetGraph addAll(Collection<ColumnCombinationBitset> columnCombinations)
      throws ColumnIndexOutOfBoundsException {
    for (ColumnCombinationBitset columnCombination : columnCombinations) {
      add(columnCombination);
    }

    return this;
  }

  /**
   * @return an {@link ArrayList} of all {@link ColumnCombinationBitset}s
   * contained in the {@link SubSetGraph}
   */
  public ArrayList<ColumnCombinationBitset> getContainedSets() {
    return getContainedSets(new ColumnCombinationBitset());
  }

  /**
   * Returns an {@link ArrayList} of all {@link ColumnCombinationBitset}s
   * contained in the {@link SubSetGraph}. All results are prefixed with the result prefix (for sub
   * graph queries).
   *
   * @param resultPrefix the prefix to add to all result sets
   * @return an {@link ArrayList} of all {@link ColumnCombinationBitset}s
   * contained in the {@link SubSetGraph}
   */
  public ArrayList<ColumnCombinationBitset> getContainedSets(ColumnCombinationBitset resultPrefix) {
    ArrayList<ColumnCombinationBitset> containedSets = new ArrayList<>();

    Queue<SearchTask> openTasks = new LinkedList<>();
    openTasks.add(new SearchTask(this, -1, resultPrefix));

    while (!openTasks.isEmpty()) {
      SearchTask currentTask = openTasks.remove();

      if (currentTask.subGraph.subSetEnds) {
        containedSets.add(new ColumnCombinationBitset(currentTask.path));
      }

      for (int columnIndex = 0; columnIndex < currentTask.subGraph.subGraphs.length;
           columnIndex++) {
        SubSetGraph subGraph = currentTask.subGraph.subGraphs[columnIndex];
        if (subGraph == null) {
          continue;
        }


        openTasks.add(new SearchTask(
            subGraph,
            -1,
            new ColumnCombinationBitset(currentTask.path).addColumn(columnIndex)));
      }
    }

    return containedSets;
  }

  /**
   * Removes a column combination from the graph. Returns, whether an element was removed.
   * @param columnCombination a column combination to remove
   * @return whether the column combination was removed
   */
  public boolean remove(ColumnCombinationBitset columnCombination) {
    Stack<SubSetGraph> previousSubGraphs = new Stack<>();
    Stack<Integer> previousSubGraphIndices = new Stack<>();

    // Find correct graph.
    SubSetGraph subGraph = this;
    previousSubGraphs.push(this);
    for (int columnIndex : columnCombination.getSetBits()) {
      subGraph = subGraph.subGraphs[columnIndex];
      if (subGraph == null) {
        return false;
      }
      previousSubGraphs.push(subGraph);
      previousSubGraphIndices.push(columnIndex);
    }

    // Prune empty subgraphs.
    subGraph = previousSubGraphs.pop();
    subGraph.subSetEnds = false;
    while ((!subGraph.subSetEnds) && (subGraph.isEmpty())) {
      subGraph = previousSubGraphs.pop();
      subGraph.subGraphs[previousSubGraphIndices.pop()] = null;
    }

    return true;
  }

  /**
   * Looks for the subgraph or builds and adds a new one.
   * @param setColumnIndex the column index to perform the lookup on
   * @return the subgraph behind the column index
   */
  protected SubSetGraph lazySubGraphGeneration(int setColumnIndex) {
    SubSetGraph subGraph = subGraphs[setColumnIndex];

    if (subGraph == null) {
      subGraph = new SubSetGraph(subGraphs.length);
      subGraphs[setColumnIndex] = subGraph;
    }

    return subGraph;
  }

  /**
   * Returns all subsets of the given column combination that are in the graph.
   * @param superset given superset to search for subsets
   * @return a list containing all found subsets
   */
  // TODO(zwiener): Does this include equivalent sets?
  public ArrayList<ColumnCombinationBitset> getExistingSubsets(
      ColumnCombinationBitset superset)
  {
    ArrayList<ColumnCombinationBitset> subsets = new ArrayList<>();
    if (this.isEmpty()) {
      return subsets;
    }
    // Create task queue and initial task.
    Queue<SearchTask> openTasks = new LinkedList<>();
    openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));

    while (!openTasks.isEmpty()) {
      SearchTask currentTask = openTasks.remove();
      // If the current subgraph is empty a subset has been found
      if (currentTask.subGraph.isEmpty()) {
        subsets.add(new ColumnCombinationBitset(currentTask.path));
        continue;
      }

      if (currentTask.subGraph.subSetEnds) {
        subsets.add(new ColumnCombinationBitset(currentTask.path));
      }

      // Iterate over the remaining column indices
      for (int i = currentTask.numberOfCheckedColumns; i < superset.size(); i++) {
        int currentColumnIndex = superset.getSetBits().get(i);
        // Get the subgraph behind the current index
        SubSetGraph subGraph =
            currentTask.subGraph.subGraphs[currentColumnIndex];
        // column index is not set on any set --> check next column index
        if (subGraph != null) {
          // Add the current column index to the path
          ColumnCombinationBitset path =
            new ColumnCombinationBitset(currentTask.path)
              .addColumn(currentColumnIndex);

          openTasks.add(new SearchTask(subGraph, i + 1, path));
        }
      }
    }

    return subsets;
  }

  /**
   * Returns whether at least one subset is contained in the graph. The method returns when the
   * first subset is found in the graph. This is possibly faster than
   * {@link SubSetGraph#getExistingSubsets(ColumnCombinationBitset)}, because a smaller part of the
   * graph needs be traversed.
   * @param superset the set for which the graph should be checked for subsets
   * @return whether at least one subset is contained in the graph
   */
  public boolean containsSubset(ColumnCombinationBitset superset) {
    if (this.isEmpty()) {
      return false;
    }
    Queue<SearchTask> openTasks = new LinkedList<>();
    openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));

    while (!openTasks.isEmpty()) {
      SearchTask currentTask = openTasks.remove();
      // If the current subgraph is empty a subset has been found
      if (currentTask.subGraph.isEmpty()) {
        return true;
      }

      if (currentTask.subGraph.subSetEnds) {
        return true;
      }

      // Iterate over the remaining column indices
      for (int i = currentTask.numberOfCheckedColumns; i < superset.size(); i++) {
        int currentColumnIndex = superset.getSetBits().get(i);
        // Get the subgraph behind the current index
        SubSetGraph subGraph =
            currentTask.subGraph.subGraphs[currentColumnIndex];
        // column index is not set on any set --> check next column index
        if (subGraph != null) {
          // Add the current column index to the path
          ColumnCombinationBitset path =
            new ColumnCombinationBitset(currentTask.path)
              .addColumn(currentColumnIndex);

          openTasks.add(new SearchTask(subGraph, i + 1, path));
        }
      }
    }

    return false;
  }

  /**
   * The method returns all minimal subsets contained in the graph using a breadth-first search
   * pattern. Non minimal subsets are not traversed.
   * @return a list containing all minimal subsets
   */
  public Set<ColumnCombinationBitset> getMinimalSubsets() throws ColumnIndexOutOfBoundsException {
    if (this.isEmpty()) {
      return new TreeSet<>();
    }

    SubSetGraph graph = new SubSetGraph(this.subGraphs.length);
    TreeSet<ColumnCombinationBitset> result = new TreeSet<>();
    TreeSet<SearchTask> openTasks = new TreeSet<>();
    openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));

    while (!openTasks.isEmpty()) {
      SearchTask currentTask = openTasks.pollFirst();
      if (currentTask.subGraph.subSetEnds) {
        if (!graph.containsSubset(currentTask.path)) {
          graph.add(currentTask.path);
          result.add(currentTask.path);
        }
      }
      else {
        // Iterate over the remaining column indices
        for (int columnIndex = 0; columnIndex < currentTask.subGraph.subGraphs.length;
             columnIndex++) {
          // Get the subgraph behind the current index
          SubSetGraph subGraph =
              currentTask.subGraph.subGraphs[columnIndex];
          // column index is not set on any set --> check next column index
          if (subGraph != null) {
            // Add the current column index to the path
            ColumnCombinationBitset path =
              new ColumnCombinationBitset(currentTask.path)
                  .addColumn(columnIndex);

            openTasks
                .add(new SearchTask(subGraph, columnIndex + 1, path));
          }
        }
      }
    }
    return result;
  }

  /**
   * Returns all supersets of the given column combination that are in the graph.
   *
   * @param subset given subset to search for supersets
   * @return a list containing all found supersets
   */
  public ArrayList<ColumnCombinationBitset> getExistingSupersets(ColumnCombinationBitset subset) {
    ArrayList<ColumnCombinationBitset> subsets = new ArrayList<>();

    if (this.isEmpty()) {
      return subsets;
    }

    Queue<SearchTask> openTasks = new LinkedList<>();
    openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));

    while (!openTasks.isEmpty()) {
      SearchTask currentTask = openTasks.remove();

      List<Integer> setBits = subset.getSetBits();
      if (setBits.size() <= currentTask.numberOfCheckedColumns) {
        subsets.addAll(currentTask.subGraph.getContainedSets(currentTask.path));
        continue;
      }
      int from;
      if (currentTask.numberOfCheckedColumns == 0) {
        from = 0;
      } else {
        from = setBits.get(currentTask.numberOfCheckedColumns - 1);
      }

      // Only column identifiers coming after the current identifier are relevant, or all remaining.
      int upto = Math.min(setBits.get(currentTask.numberOfCheckedColumns) + 1, subGraphs.length);

      for (int columnIndex = from; columnIndex < upto; columnIndex++) {
        SubSetGraph subGraph = currentTask.subGraph.subGraphs[columnIndex];
        if (subGraph == null) {
          continue;
        }

        if (columnIndex == setBits.get(currentTask.numberOfCheckedColumns)) {
          openTasks.add(new SearchTask(
              subGraph,
              currentTask.numberOfCheckedColumns + 1,
              new ColumnCombinationBitset(currentTask.path).addColumn(columnIndex)));
        } else {
          openTasks.add(new SearchTask(
              subGraph,
              currentTask.numberOfCheckedColumns,
              new ColumnCombinationBitset(currentTask.path).addColumn(columnIndex)));
        }
      }
    }

    return subsets;
  }

  /**
   * Returns whether at least one superset is contained in the graph.The method returns when the
   * first superset is found in the graph. This is possibly faster than
   * {@link SubSetGraph#getExistingSupersets(ColumnCombinationBitset)}, because a smaller part of
   * the graph needs to be traversed.
   *
   * @param subset the set for which the graph should be checked for supersets
   * @return whether at least one superset is contained in the graph
   */
  public boolean containsSuperset(ColumnCombinationBitset subset) {
    if (this.isEmpty()) {
      return false;
    }

    Queue<SearchTask> openTasks = new LinkedList<>();
    openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));

    while (!openTasks.isEmpty()) {
      SearchTask currentTask = openTasks.remove();

      List<Integer> setBits = subset.getSetBits();
      if (setBits.size() <= currentTask.numberOfCheckedColumns) {
        return true;
      }
      int from;
      if (currentTask.numberOfCheckedColumns == 0) {
        from = 0;
      } else {
        from = setBits.get(currentTask.numberOfCheckedColumns - 1);
      }

      // Only column identifiers coming after the current identifier are relevant, or all remaining.
      int upto = Math.min(setBits.get(currentTask.numberOfCheckedColumns) + 1, subGraphs.length);

      for (int columnIndex = from; columnIndex < upto; columnIndex++) {
        SubSetGraph subGraph = currentTask.subGraph.subGraphs[columnIndex];
        if (subGraph == null) {
          continue;
        }

        if (columnIndex == setBits.get(currentTask.numberOfCheckedColumns)) {
          openTasks.add(new SearchTask(
              subGraph,
              currentTask.numberOfCheckedColumns + 1,
              currentTask.path.addColumn(columnIndex)));
        } else {
          openTasks.add(new SearchTask(
              subGraph,
              currentTask.numberOfCheckedColumns,
              currentTask.path.addColumn(columnIndex)));
        }
      }
    }

    return false;
  }

  /**
   * @return whether the graph is empty
   */
  public boolean isEmpty() {
    for (SubSetGraph subGraph : subGraphs) {
      if (subGraph != null) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SubSetGraph that = (SubSetGraph) o;

    if (subSetEnds != that.subSetEnds) {
      return false;
    }
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    return Arrays.equals(subGraphs, that.subGraphs);

  }

  @Override
  public int hashCode() {
    int result = subGraphs != null ? Arrays.hashCode(subGraphs) : 0;
    result = 31 * result + (subSetEnds ? 1 : 0);
    return result;
  }

  @Override public String toString() {
    List<String> rows = new ArrayList<>();

    stringRepresentation(rows, 0, 0);

    return Joiner.on('\n').join(rows.subList(0, rows.size() - 1));
  }

  /**
   * Recursive generation of a string representation of the graph.
   * @param rows the rows of the representation
   * @param level the current row level to write to
   * @param leftMargin how many spaces to leave at the left
   * @return the number of columns written to in the current row
   */
  protected int stringRepresentation(List<String> rows, int level, int leftMargin) {
    int numberOfColumnsWritten;
    if (level >= rows.size()) {
      rows.add("");
    }
    StringBuilder row = new StringBuilder(rows.get(level));
    for (int columnIndex = 0; columnIndex < subGraphs.length; columnIndex++) {
      SubSetGraph subGraph = subGraphs[columnIndex];
      if (subGraph == null) {
        continue;
      }
      while (row.length() < leftMargin) {
        row.append(" ");
      }
      int newLeftMargin = row.length();
      row.append(columnIndex);
      if (subGraph.subSetEnds) {
        row.append("X");
      }
      row.append(" ");
      numberOfColumnsWritten = subGraph.stringRepresentation(rows, level + 1, newLeftMargin);
      while (row.length() < numberOfColumnsWritten) {
        row.append(" ");
      }
    }
    rows.set(level, CharMatcher.WHITESPACE.trimTrailingFrom(row));

    return row.length();
  }
}

/**
 * Task used to find subsets (avoiding recursion).
 */
class SearchTask implements Comparable<SearchTask> {

  public SubSetGraph subGraph;
  public int numberOfCheckedColumns;
  public ColumnCombinationBitset path;

  public SearchTask(
      SubSetGraph subGraph,
      int numberOfCheckedColumns,
      ColumnCombinationBitset path)
  {
    this.subGraph = subGraph;
    this.numberOfCheckedColumns = numberOfCheckedColumns;
    this.path = path;
  }

  @Override
  public int compareTo(SearchTask o) {
    return this.path.compareTo(o.path);
  }
}
