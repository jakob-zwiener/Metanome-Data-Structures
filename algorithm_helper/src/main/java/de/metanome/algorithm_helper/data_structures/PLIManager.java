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

import java.util.Map;

/**
 * Manages plis and performs intersect operations.
 *
 * TODO(zwiener): Extend docs
 *
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  protected Map<ColumnCombinationBitset, PositionListIndex> plis;
  protected ColumnCombinationBitset allColumnCombination;

  /**
   * TODO docs
   */
  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> plis) {
    this.plis = plis;
    int[] allColumnIndices = new int[plis.size()];
    for (int i = 0; i < plis.size(); i++) {
      allColumnIndices[i] = i;
    }
    allColumnCombination = new ColumnCombinationBitset(allColumnIndices);
  }

  /**
   * Returns the requested PLI for the given {@link ColumnCombinationBitset}s. If multiple {@link ColumnCombinationBitset}s are given, each one is queried or built and then intersected.
   *
   * @param columnCombinations the column combinations which's PLIs are intersected
   * @return the PLI associated to the given {@link ColumnCombinationBitset}s
   * @throws PLIBuildingException when the requested PLI cannot be built
   */
  public PositionListIndex getPli(final ColumnCombinationBitset... columnCombinations)
      throws PLIBuildingException {
    if (columnCombinations.length == 0) {
      throw new PLIBuildingException("No ColumnCombinationBitset given to query for pli.");
    }

    if (columnCombinations.length == 1) {
      return getPli(columnCombinations[0]);
    }

    PositionListIndex intersect = null;
    for (ColumnCombinationBitset columnCombination : columnCombinations) {
      if (intersect == null) {
        intersect = getPli(columnCombination);
      }
      intersect = intersect.intersect(getPli(columnCombination));
    }

    return intersect;
  }

  protected PositionListIndex getPli(final ColumnCombinationBitset columnCombination)
      throws PLIBuildingException {
    if (!columnCombination.isSubsetOf(allColumnCombination)) {
      throw new PLIBuildingException(
          "The column combination should only contain column indices of plis that are known to the pli manager.");
    }

    PositionListIndex result = plis.get(columnCombination);
    if (result != null) {
      return result;
    }

    PositionListIndex intersect = null;
    ColumnCombinationBitset unionColumnCombination = null;
    for (ColumnCombinationBitset oneColumnCombination : columnCombination
        .getContainedOneColumnCombinations()) {
      if (intersect == null) {
        intersect = plis.get(oneColumnCombination);
        unionColumnCombination = oneColumnCombination;
        continue;
      }
      intersect = intersect.intersect(plis.get(oneColumnCombination));
      unionColumnCombination = unionColumnCombination.union(oneColumnCombination);
      plis.put(unionColumnCombination, intersect);
    }

    return intersect;
  }

}
