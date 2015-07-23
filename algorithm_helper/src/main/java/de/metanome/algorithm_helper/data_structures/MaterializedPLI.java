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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * The materialized form of a {@link PositionListIndex}. This is an intermediate representation needed while
 * intersecting {@link PositionListIndex}es.
 * @author Jakob Zwiener
 * @see Partition
 * @see PositionListIndex
 */
public class MaterializedPLI implements Partition {

  protected int[] dataRepresentatives;

  public MaterializedPLI(PositionListIndex pli) {
    dataRepresentatives = asArray(pli);
  }

  /**
   * Materializes the PLI to an int array of row value representatives. The position list index ((0, 1),
   * (2, 4), (3, 5)) would be represented by [1, 1, 2, 3, 2, 3].
   * @return the pli as list
   */
  protected int[] asArray(PositionListIndex pli) {
    int[] materializedPli = new int[pli.getNumberOfRows()];
    int uniqueValueCount = SINGLETON_VALUE + 1;
    for (IntArrayList sameValues : pli.getClusters()) {
      for (int rowIndex : sameValues) {
        materializedPli[rowIndex] = uniqueValueCount;
      }
      uniqueValueCount++;
    }

    return materializedPli;
  }

  @Override public PositionListIndex intersect(final PositionListIndex otherPli) {
    return calculateIntersection(otherPli);
  }

  @Override public int getNumberOfRows() {
    return dataRepresentatives.length;
  }

  /**
   * Intersects the two given {@link PositionListIndex} and returns the outcome as new
   * PositionListIndex.
   * @param otherPli the other {@link PositionListIndex} to intersect
   * @return the intersected {@link PositionListIndex}
   */
  protected PositionListIndex calculateIntersection(PositionListIndex otherPli) {
    Map<IntPair, IntArrayList> map = new HashMap<>();
    buildMap(otherPli, map);

    List<IntArrayList> clusters = new ArrayList<>();
    for (IntArrayList cluster : map.values()) {
      if (cluster.size() < 2) {
        continue;
      }
      clusters.add(cluster);
    }
    return new PositionListIndex(clusters, getNumberOfRows());
  }

  protected void buildMap(PositionListIndex otherPli, Map<IntPair, IntArrayList> map)
  {
    int uniqueValueCount = 0;
    for (IntArrayList sameValues : otherPli.clusters) {
      for (int rowCount : sameValues) {
        if ((getNumberOfRows() > rowCount) &&
          (dataRepresentatives[rowCount] != SINGLETON_VALUE)) {
          IntPair pair = new IntPair(uniqueValueCount, dataRepresentatives[rowCount]);
          updateMap(map, rowCount, pair);
        }
      }
      uniqueValueCount++;
    }
  }

  protected void updateMap(Map<IntPair, IntArrayList> map, int rowCount, IntPair pair) {
    if (map.containsKey(pair)) {
      IntArrayList currentList = map.get(pair);
      currentList.add(rowCount);
    }
    else {
      IntArrayList newList = new IntArrayList();
      newList.add(rowCount);
      map.put(pair, newList);
    }
  }

}
