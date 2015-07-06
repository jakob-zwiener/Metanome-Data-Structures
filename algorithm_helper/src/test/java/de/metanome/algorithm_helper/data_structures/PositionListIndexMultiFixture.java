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
import java.util.List;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * @author Jakob Zwiener
 */
public class PositionListIndexMultiFixture {

  public PositionListIndex getFirstPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 2, 4, 8 };
    clusters.add(new IntArrayList(cluster1));

    int[] cluster2 = { 1, 3, 12 };
    clusters.add(new IntArrayList(cluster2));

    int[] cluster3 = { 0, 7, 9, 11 };
    clusters.add(new IntArrayList(cluster3));

    return new PositionListIndex(clusters, 13);
  }

  public PositionListIndex getSecondPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 2, 4, 8 };
    clusters.add(new IntArrayList(cluster1));

    int[] cluster2 = { 1, 3 };
    clusters.add(new IntArrayList(cluster2));

    int[] cluster3 = { 0, 7, 9, 11, 12 };
    clusters.add(new IntArrayList(cluster3));

    return new PositionListIndex(clusters, 13);
  }

  public PositionListIndex getThirdPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 2, 4, 8 };
    clusters.add(new IntArrayList(cluster1));

    int[] cluster2 = { 3, 12 };
    clusters.add(new IntArrayList(cluster2));

    int[] cluster3 = { 7, 9, 11 };
    clusters.add(new IntArrayList(cluster3));

    return new PositionListIndex(clusters, 13);
  }

  public PositionListIndex[] getOtherPLIs() {
    PositionListIndex[] plis = new PositionListIndex[2];

    plis[0] = getSecondPLI();
    plis[1] = getThirdPLI();

    return plis;
  }

  public PositionListIndex getExpectedIntersectedPLI() {
    List<IntArrayList> clusters = new ArrayList<>();

    int[] cluster1 = { 2, 4, 8 };
    clusters.add(new IntArrayList(cluster1));

    int[] cluster2 = { 7, 9, 11 };
    clusters.add(new IntArrayList(cluster2));

    return new PositionListIndex(clusters, 13);
  }

}
