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

  public long searchTimes;
  public long intersectTimes;
  public int numberOfIntersects;
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

    PositionListIndex exactPli = plis.get(columnCombination);
    if (exactPli != null) {
      return exactPli;
    }

    List<ColumnCombinationBitset> subsets = pliGraph.getExistingSubsets(columnCombination);

    // System.out.println(subsets.size());

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
          // System.out.println("equal--------");
          // System.out.println(String.format("best: %d current: %d", plis.get(bestSubset).getRawKeyError(), plis.get(
          //   subset).getRawKeyError()));
          if (plis.get(bestSubset).getRawKeyError() > plis.get(subset).getRawKeyError()) {
            bestSubset = subset;
            bestUnion = currentUnion;
          }
        }
      }
      solution.add(bestSubset);
      unionSoFar = unionSoFar.union(bestSubset);
    }

    // System.out.println(solution.size());

    // long beforeIntersect = System.nanoTime();

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
      plis.put(unionCombination, intersect);
      pliGraph.add(unionCombination);
    }

    // long intersectTime = System.nanoTime() - beforeIntersect;
    // this.intersectTimes += intersectTime;
    // System.out.println(String.format("%fs intersect time", intersectTimes / 1000000000d));
    // System.out.println(String.format("%d intersects", numberOfIntersects++));


/*
      ColumnCombinationBitset difference = columnCombination.minus(subset);
      PositionListIndex differencePli = plis.get(difference);
      if (differencePli != null) {
        System.out.println("found--------");
        return differencePli.intersect(plis.get(subset));
      }
    }*/


    /*long startSearch = System.nanoTime();

    PositionListIndex minPli = null;
    int minKeyError = Integer.MAX_VALUE;
    // FIXME(zwiener): A null pointer exception could originate here. Add test for the case that no subsets are found.
    ColumnCombinationBitset minKeyErrorSubset = null;
    for (ColumnCombinationBitset subset : subsets) {
      PositionListIndex pli = plis.get(subset);
      if (pli.getRawKeyError() < minKeyError) {
        minPli = pli;
        minKeyErrorSubset = subset;
        minKeyError = pli.getRawKeyError();
      }
    }
    searchTimes += (System.nanoTime() - startSearch);

    long beforeIntersect = System.nanoTime();

    ColumnCombinationBitset missingColumns = columnCombination.minus(minKeyErrorSubset);

    PositionListIndex intersect = minPli;
    ColumnCombinationBitset currentColumnCombination = minKeyErrorSubset;
    System.out.println(missingColumns);
    int intersects = 0;
    for (ColumnCombinationBitset oneColumnCombination : missingColumns.getContainedOneColumnCombinations()) {
      intersect = intersect.intersect(plis.get(oneColumnCombination));
      intersects++;
      // TODO(zwiener): Whether plis get cached or not is currently not tested.
      currentColumnCombination = currentColumnCombination.union(oneColumnCombination);
      plis.put(currentColumnCombination, intersect);
      pliGraph.add(currentColumnCombination);
    }

    long intersectTime = System.nanoTime() - beforeIntersect;
    this.intersectTimes += intersectTime;
    System.out.println(String.format("%d intra intersects", intersects));
    System.out.println(String.format("%fs intersect time", intersectTimes / 1000000000d));
    System.out.println(String.format("%d intersects", numberOfIntersects++));*/

    return intersect;
  }


}
