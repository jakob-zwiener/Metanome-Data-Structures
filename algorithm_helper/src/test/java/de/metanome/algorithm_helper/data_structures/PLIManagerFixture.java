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
 * @author Jakob Zwiener
 */
public class PLIManagerFixture {

  protected int[][][] pliClusters = {
    { { 0, 2, 3, 5 }, { 1, 7, 10 } },
    { { 1, 9 }, { 0, 2, 3 }, { 8, 11 } },
    { { 0, 1 }, { 2, 3 }, { 4, 5 }, { 6, 7 }, { 8, 9 }, { 10, 11 } },
  };

  public Map<ColumnCombinationBitset, PositionListIndex> getPlis() {
    Map<ColumnCombinationBitset, PositionListIndex> plis = new HashMap<>();

    int columnIndex = 0;
    for (int[][] clusters : pliClusters) {
      plis.put(new ColumnCombinationBitset(columnIndex++), buildPliFromClusters(clusters));
    }

    return plis;
  }

  protected int numberOfRows() {
    return 12;
  }

  protected PositionListIndex buildPliFromClusters(int[][] clusters) {
    List<IntArrayList> clusterList = new ArrayList<>();
    for (int[] cluster : clusters) {
      clusterList.add(new IntArrayList(cluster));
    }

    return new PositionListIndex(clusterList, numberOfRows());
  }

  public PositionListIndex getExpectedIntersect012() {
    int[][] clusters = { { 2, 3 } };

    return buildPliFromClusters(clusters);
  }
}
