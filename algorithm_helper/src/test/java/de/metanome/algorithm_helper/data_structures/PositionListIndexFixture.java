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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class PositionListIndexFixture {

  public PositionListIndex getFirstPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 2, 4, 8 };
    clusters.add(new IntArrayList(cluster1));
    int[] cluster2 = { 5, 6, 7 };
    clusters.add(new IntArrayList(cluster2));

    return new PositionListIndex(clusters, 9);
  }

  public String getExpectedFirstPliToString() {
    return "PositionListIndex{clusters=[[2, 4, 8], [5, 6, 7]], numberOfRows=9, rawKeyError=4}";
  }

  public int getExpectedFirstPLIRawKeyError() {
    return 4;
  }

  public int getExpectedFirstPliSumClusterSize() {
    return 6;
  }

  public int getFirstPLISize() {
    return getFirstPLI().clusters.size();
  }

  protected PositionListIndex getPermutatedFirstPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 7, 6, 5 };
    clusters.add(new IntArrayList(cluster1));
    int[] cluster2 = { 4, 2, 2, 8 };
    clusters.add(new IntArrayList(cluster2));

    return new PositionListIndex(clusters, 9);
  }

  protected PositionListIndex getSupersetOfFirstPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 7, 6, 5 };
    clusters.add(new IntArrayList(cluster1));
    int[] cluster2 = { 4, 2, 2, 8 };
    clusters.add(new IntArrayList(cluster2));
    int[] cluster3 = { 10, 11 };
    clusters.add(new IntArrayList(cluster3));

    return new PositionListIndex(clusters, 12);
  }

  protected Int2IntOpenHashMap getFirstPLIAsHashMap() {
    Int2IntOpenHashMap pliMap = new Int2IntOpenHashMap();

    pliMap.addTo(5, 1);
    pliMap.addTo(7, 1);
    pliMap.addTo(6, 1);

    pliMap.addTo(2, 0);
    pliMap.addTo(4, 0);
    pliMap.addTo(8, 0);

    return pliMap;
  }

  public int[] getFirstPLIAsArray() {
    return new int[] {
        PositionListIndex.SINGLETON_VALUE,
        PositionListIndex.SINGLETON_VALUE,
        1,
        PositionListIndex.SINGLETON_VALUE,
        1,
        2,
        2,
        2,
        1
    };
  }

  protected PositionListIndex getSecondPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 1, 2, 5, 8 };
    clusters.add(new IntArrayList(cluster1));
    int[] cluster2 = { 4, 6, 7 };
    clusters.add(new IntArrayList(cluster2));

    return new PositionListIndex(clusters, 9);
  }

  public String getExpectedSecondPliToString() {
    return "PositionListIndex{clusters=[[1, 2, 5, 8], [4, 6, 7]], numberOfRows=9, rawKeyError=5}";
  }

  public int getExpectedSecondPLIRawKeyError() {
    return 5;
  }

  public int getExpectedSecondPliSumClusterSize() {
    return 7;
  }

  protected PositionListIndex getExpectedIntersectedPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 2, 8 };
    clusters.add(new IntArrayList(cluster1));
    int[] cluster2 = { 6, 7 };
    clusters.add(new IntArrayList(cluster2));

    return new PositionListIndex(clusters, 9);
  }

  public int getExpectedIntersectedPLIRawKeyError() {
    return 2;
  }

}
