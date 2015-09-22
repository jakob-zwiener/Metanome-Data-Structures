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

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  protected Map<ColumnCombinationBitset, PositionListIndex> plis;
  protected ColumnCombinationBitset allColumnCombination;

  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> plis) {
    this.plis = plis;
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

    PriorityQueue<ColumnCombinationBitset> pq = new PriorityQueue<ColumnCombinationBitset>(
      columnCombination.size(),
      new Comparator<ColumnCombinationBitset>() {
        @Override public int compare(final ColumnCombinationBitset o1, final ColumnCombinationBitset o2) {
          return plis.get(o1).getRawKeyError() - plis.get(o2).getRawKeyError();
        }
      });

    pq.addAll(columnCombination.getContainedOneColumnCombinations());

    ColumnCombinationBitset unionSoFar = pq.poll();
    PositionListIndex intersect = plis.get(unionSoFar);
    while (!unionSoFar.equals(columnCombination)) {
      ColumnCombinationBitset currentSubset = pq.poll();

      if (currentSubset.isSubsetOf(unionSoFar)) {
        continue;
      }

      unionSoFar = unionSoFar.union(currentSubset);
      intersect = intersect.intersect(plis.get(currentSubset));
    }

    return intersect;
  }


}
