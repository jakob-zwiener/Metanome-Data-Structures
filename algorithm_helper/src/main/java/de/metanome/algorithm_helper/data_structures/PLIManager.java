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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  // FIXME(zwiener): Make this protected.
  public Map<ColumnCombinationBitset, PositionListIndex> plis;
  protected ColumnCombinationBitset allColumnCombination;
  protected SubSetGraph pliSubSetGraph;
  protected SuperSetGraph pliSuperSetGraph;

  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> plis) {
    this.plis = plis;
    pliSubSetGraph = new SubSetGraph();
    pliSubSetGraph.addAll(plis.keySet());
    pliSuperSetGraph = new SuperSetGraph(plis.size());
    pliSuperSetGraph.addAll(plis.keySet());
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

    PositionListIndex exactPli = plis.get(columnCombination);
    if (exactPli != null) {
      return exactPli;
    }

    List<ColumnCombinationBitset> subsets = pliSubSetGraph.getExistingSubsets(columnCombination);

    // Calculate set cover.
    ColumnCombinationBitset unionSoFar = new ColumnCombinationBitset();
    ColumnCombinationBitset bestSubset = new ColumnCombinationBitset();
    ColumnCombinationBitset bestUnion = new ColumnCombinationBitset();
    List<ColumnCombinationBitset> solution = new LinkedList<>();
    while (!unionSoFar.equals(columnCombination)) {
      for (ColumnCombinationBitset subset : subsets) {

        ColumnCombinationBitset currentUnion = unionSoFar.union(subset);

        if (currentUnion.size() > bestUnion.size()) {
          bestSubset = subset;
          bestUnion = currentUnion;
        }
        else if (currentUnion.size() == bestUnion.size()) {
          // If multiple subsets add the same number of columns use key error as tiebreaker.
          if (plis.get(bestSubset).getRawKeyError() > plis.get(subset).getRawKeyError()) {
            bestSubset = subset;
            bestUnion = currentUnion;
          }
        }
      }
      solution.add(bestSubset);
      unionSoFar = unionSoFar.union(bestSubset);
    }

    // Perform the necessary intersections.
    PositionListIndex intersect = null;
    ColumnCombinationBitset unionCombination = null;
    for (ColumnCombinationBitset subsetCombination : solution) {
      if (intersect == null) {
        intersect = plis.get(subsetCombination);
        unionCombination = subsetCombination;
        continue;
      }

      intersect = intersect.intersect(plis.get(subsetCombination));
      unionCombination = unionCombination.union(subsetCombination);
      addPli(unionCombination, intersect);
    }

    return intersect;
  }

  protected void addPli(final ColumnCombinationBitset columnCombination,
                        final PositionListIndex intersect)
  {
    plis.put(columnCombination, intersect);
    pliSubSetGraph.add(columnCombination);
    pliSuperSetGraph.add(columnCombination);

    // System.out.println(pliSubSetGraph);

    List<ColumnCombinationBitset> supersets = pliSuperSetGraph.getExistingSupersets(columnCombination);

    for (ColumnCombinationBitset superset : supersets) {
      if (!superset.equals(columnCombination)) {
        removePli(superset);
      }
    }

    // System.out.println(pliSubSetGraph);
  }

  protected void removePli(final ColumnCombinationBitset columnCombination) {
    plis.remove(columnCombination);
    pliSubSetGraph.remove(columnCombination);
    pliSuperSetGraph.remove(columnCombination);
  }


}
