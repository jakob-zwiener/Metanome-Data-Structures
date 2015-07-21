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

import java.util.List;
import java.util.Map;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  public long searchTime;
  protected Map<ColumnCombinationBitset, PositionListIndex> plis;
  protected ColumnCombinationBitset allColumnCombination;
  protected SubSetGraph pliGraph;

  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> plis) {
    this.plis = plis;
    pliGraph = new SubSetGraph();
    pliGraph.addAll(plis.keySet());
    int[] allColumnIndices = new int[plis.size()];
    for (int i = 0; i < plis.size(); i++) {
      allColumnIndices[i] = i;
    }
    allColumnCombination = new ColumnCombinationBitset(allColumnIndices);
  }

  public PositionListIndex buildPli(final ColumnCombinationBitset columnCombination) throws PLIBuildingException {
    if (!columnCombination.isSubsetOf(allColumnCombination)) {
      throw new PLIBuildingException(
        "The column combination should only contain column indices of plis that are known to the pli manager.");
    }


    long startSearch = System.nanoTime();
    List<ColumnCombinationBitset> subsets = pliGraph.getExistingSubsets(columnCombination);
    PositionListIndex minPli = null;
    int minKeyError = Integer.MAX_VALUE;
    // FIXME(zwiener): A null pointer exception could originate here. Add test for the case that no subsets are found.
    ColumnCombinationBitset minKeyErrorSubset = null;
    for (ColumnCombinationBitset subset : subsets) {
      PositionListIndex pli = plis.get(subset);
      if (pli.getRawKeyError() < minKeyError) {
        minPli = pli;
        minKeyErrorSubset = subset;
      }
    }
    searchTime += (System.nanoTime() - startSearch);

    ColumnCombinationBitset missingColumns = columnCombination.minus(minKeyErrorSubset);

    PositionListIndex intersect = minPli;
    ColumnCombinationBitset currentColumnCombination = minKeyErrorSubset;
    System.out.println(missingColumns);
    for (ColumnCombinationBitset oneColumnCombination : missingColumns.getContainedOneColumnCombinations()) {
      intersect = intersect.intersect(plis.get(oneColumnCombination));
      // TODO(zwiener): Whether plis get cached or not is currently not tested.
      currentColumnCombination = currentColumnCombination.union(oneColumnCombination);
      plis.put(currentColumnCombination, intersect);
      pliGraph.add(currentColumnCombination);
    }

    return intersect;
  }


}
