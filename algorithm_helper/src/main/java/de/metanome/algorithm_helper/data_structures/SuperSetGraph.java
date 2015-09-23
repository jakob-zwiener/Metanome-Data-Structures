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

import java.util.ArrayList;
import java.util.Collection;

/**
 * A graph that allows for efficient lookup of all supersets in a graph structure for a given {@link
 * de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset} subset. This class acts as
 * a wrapper for {@link SubSetGraph} by inverting all input and output {@link
 * de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset}s to find supersets instead
 * of subsets.
 * @author Jens Ehrlich
 */

public class SuperSetGraph {

  protected int numberOfColumns = -1;
  protected SubSetGraph graph;

  public SuperSetGraph(int numberOfColumns) {
    // TODO(zwiener): Rename numberOfColumns to dimension.
    this.numberOfColumns = numberOfColumns;
    graph = new SubSetGraph(numberOfColumns);
  }

  /**
   * Adds a {@link de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset} to the
   * graph.
   * @param columnCombination the {@link de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset}
   * to add
   * @return the graph
   */
  public SuperSetGraph add(ColumnCombinationBitset columnCombination) {
    graph.add(columnCombination.invert(numberOfColumns));
    return this;
  }

  /**
   * Adds  all {@link de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset} to the
   * graph
   * @param columnCombinations to be added to the graph
   * @return the graph
   */
  public SuperSetGraph addAll(Collection<ColumnCombinationBitset> columnCombinations) {
    for (ColumnCombinationBitset columnCombination : columnCombinations) {
      graph.add(columnCombination.invert(numberOfColumns));
    }
    return this;
  }

  public boolean remove(ColumnCombinationBitset columnCombination) {
    return graph.remove(columnCombination.invert(numberOfColumns));
  }

  /**
   * Returns all supersets of the given {@link de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset}
   * that are in the graph
   * @param subset given subset to search for supersets
   * @return a list containing all found supersets
   */
  public ArrayList<ColumnCombinationBitset> getExistingSupersets(ColumnCombinationBitset subset) {
    ArrayList<ColumnCombinationBitset> result = new ArrayList<>();
    ArrayList<ColumnCombinationBitset>
      invertedColumns =
      graph.getExistingSubsets(subset.invert(numberOfColumns));

    for (ColumnCombinationBitset bitset : invertedColumns) {
      result.add(bitset.invert(numberOfColumns));
    }
    return result;
  }

  /**
   * The method returns when the first superset is found in the graph. This is possibly faster than
   * {@link SuperSetGraph#getExistingSupersets(ColumnCombinationBitset)}, because a smaller part of
   * the graph must be traversed.
   * @return whether at least a single superset is contained in the graph
   */
  public boolean containsSuperset(ColumnCombinationBitset subset) {
    return subset.isEmpty() || graph.containsSubset(subset.invert(numberOfColumns));
  }

  /**
   * @return whether the graph is empty
   */
  public boolean isEmpty() {
    return this.graph.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SuperSetGraph graph1 = (SuperSetGraph) o;

    return numberOfColumns == graph1.numberOfColumns && graph.equals(graph1.graph);
  }

  @Override
  public int hashCode() {
    int result = numberOfColumns;
    result = 31 * result + graph.hashCode();
    return result;
  }
}
