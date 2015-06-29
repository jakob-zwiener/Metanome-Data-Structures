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
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * Position list indices (or stripped partitions) are an index structure that stores the positions
 * of equal values in a nested list. A column with the values a, a, b, c, b, c transfers to the
 * position list index ((0, 1), (2, 4), (3, 5)). Clusters of size 1 are discarded. A position list
 * index should be created using the {@link PLIBuilder}.
 */
public class PositionListIndex implements Serializable {

  public static final transient int SINGLETON_VALUE = 0;
  private static final long serialVersionUID = 2;
  // TODO(zwiener): Make thread pool size accessible from the outside.
  public static transient ExecutorService exec = Executors.newFixedThreadPool(1);
  protected List<IntArrayList> clusters;
  protected int rawKeyError = -1;

  public PositionListIndex(List<IntArrayList> clusters) {
    this.clusters = clusters;
  }

  /**
   * Constructs an empty {@link PositionListIndex}.
   */
  public PositionListIndex() {
    this.clusters = new ArrayList<>();
  }

  /**
   * Intersects the given PositionListIndex with this PositionListIndex returning a new
   * PositionListIndex. For the intersection the smaller PositionListIndex is converted into a
   * HashMap.
   * @param otherPLI the other {@link PositionListIndex} to intersect
   * @return the intersected {@link PositionListIndex}
   */
  public PositionListIndex intersect(PositionListIndex otherPLI) {
    //TODO Optimize Smaller PLI as Hashmap?
    return calculateIntersection(otherPLI);
  }

  public List<IntArrayList> getClusters() {
    return clusters;
  }

  /**
   * Creates a complete (deep) copy of the {@link de.metanome.algorithm_helper.data_structures.PositionListIndex}.
   * @return cloned PositionListIndex
   */
  @Override
  public PositionListIndex clone() {
    List<IntArrayList> newClusters = new ArrayList<>();
    for (IntArrayList cluster : clusters) {
      newClusters.add(cluster.clone());
    }

    PositionListIndex clone = new PositionListIndex(newClusters);
    clone.rawKeyError = this.rawKeyError;
    return clone;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;

    List<IntOpenHashSet> setCluster = convertClustersToSets(clusters);

    Collections.sort(setCluster, new Comparator<IntSet>() {

      @Override
      public int compare(IntSet o1, IntSet o2) {
        return o1.hashCode() - o2.hashCode();
      }
    });
    result = prime * result + (setCluster.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PositionListIndex other = (PositionListIndex) obj;
    if (clusters == null) {
      if (other.clusters != null) {
        return false;
      }
    }
    else {
      List<IntOpenHashSet> setCluster = convertClustersToSets(clusters);
      List<IntOpenHashSet> otherSetCluster = convertClustersToSets(other.clusters);

      for (IntOpenHashSet cluster : setCluster) {
        if (!otherSetCluster.contains(cluster)) {
          return false;
        }
      }
      for (IntOpenHashSet cluster : otherSetCluster) {
        if (!setCluster.contains(cluster)) {
          return false;
        }
      }
    }

    return true;
  }

  protected List<IntOpenHashSet> convertClustersToSets(List<IntArrayList> listCluster) {
    List<IntOpenHashSet> setClusters = new LinkedList<>();
    for (IntList cluster : listCluster) {
      setClusters.add(new IntOpenHashSet(cluster));
    }

    return setClusters;
  }

  /**
   * Intersects the two given {@link PositionListIndex} and returns the outcome as new
   * PositionListIndex.
   * @param otherPLI the other {@link PositionListIndex} to intersect
   * @return the intersected {@link PositionListIndex}
   */
  protected PositionListIndex calculateIntersection(PositionListIndex otherPLI) {
    IntList materializedPLI = this.asList();
    Map<IntPair, IntArrayList> map = new HashMap<>();
    buildMap(otherPLI, materializedPLI, map);

    List<IntArrayList> clusters = new ArrayList<>();
    for (IntArrayList cluster : map.values()) {
      if (cluster.size() < 2) {
        continue;
      }
      clusters.add(cluster);
    }
    return new PositionListIndex(clusters);
  }

  protected void buildMap(final PositionListIndex otherPLI, final LongBigList materializedPLI,
                          final ConcurrentMap<LongPair, LongArrayList> map)
  {
    long uniqueValueCount = 0;

    List<Future<?>> tasks = new LinkedList<>();

    try {
      // System.out.println(otherPLI.clusters.size());
      for (final LongArrayList sameValues : otherPLI.clusters) {
        final long finalUniqueValueCount = uniqueValueCount;
        tasks.add(exec.submit(new Runnable() {
          @Override
          public void run() {
            final Map<LongPair, LongArrayList> internalMap = new HashMap<>();
            long start = System.nanoTime();
            // System.out.println(sameValues.size());
            for (long rowCount : sameValues) {
              // TODO(zwiener): Get is called twice.
              if ((materializedPLI.size64() > rowCount) &&
                (materializedPLI.get(rowCount) != SINGLETON_VALUE)) {
                LongPair pair = new LongPair(finalUniqueValueCount, materializedPLI.get(rowCount));

                updateMap(internalMap, rowCount, pair);
              }
            }
            map.putAll(internalMap);
            // System.out.println((System.nanoTime() - start) / 1000000000d);
          }
        }));
        uniqueValueCount++;
      }
    }
    finally {
      for (Future<?> task : tasks) {
        try {
          task.get();
        }
        catch (InterruptedException | ExecutionException e) {
          // FIXME(zwiener): Rethrow exception.
          e.printStackTrace();
        }
      }
    }
  }

  protected void updateMap(Map<LongPair, LongArrayList> map, long rowCount,
                           LongPair pair)
  {
    if (map.containsKey(pair)) {
      IntArrayList currentList = map.get(pair);
      currentList.add(rowCount);
    }
    else {
      LongArrayList newList = new LongArrayList();
      newList.add(rowCount);
      map.put(pair, newList);
    }
  }

  /**
   * Returns the position list index in a map representation. Every row index maps to a value
   * reconstruction. As the original values are unknown they are represented by a counter. The
   * position list index ((0, 1), (2, 4), (3, 5)) would be represented by {0=0, 1=0, 2=1, 3=2, 4=1,
   * 5=2}.
   * @return the pli as hash map
   */
  public Int2IntOpenHashMap asHashMap() {
    Int2IntOpenHashMap hashedPLI = new Int2IntOpenHashMap(clusters.size());
    int uniqueValueCount = 0;
    for (IntArrayList sameValues : clusters) {
      for (int rowIndex : sameValues) {
        hashedPLI.put(rowIndex, uniqueValueCount);
      }
      uniqueValueCount++;
    }
    return hashedPLI;
  }

  /**
   * Materializes the PLI to a list of row value representatives. The position list index ((0, 1),
   * (2, 4), (3, 5)) would be represented by [1, 1, 2, 3, 2, 3].
   * @return the pli as list
   */
  public IntList asList() {
    // TODO(zwiener): Initialize with approximate size.
    IntList listPli = new IntArrayList(100000000);
    int uniqueValueCount = SINGLETON_VALUE + 1;
    for (IntArrayList sameValues : clusters) {
      for (int rowIndex : sameValues) {
        addOrExtendList(listPli, uniqueValueCount, rowIndex);
      }
      uniqueValueCount++;
    }

    return listPli;
  }

  protected void addOrExtendList(IntList list, int value, int index) {
    if (list.size() <= index) {
      list.size(index + 1);
    }

    list.set(index, value);
  }

  /**
   * Returns the number of non unary clusters.
   * @return the number of clusters in the {@link PositionListIndex}
   */
  public int size() {
    return clusters.size();
  }

  /**
   * @return the {@link PositionListIndex} contains only unary clusters.
   */
  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * @return the column represented by the {@link PositionListIndex} is unique.
   */
  public boolean isUnique() {
    return isEmpty();
  }

  /**
   * Returns the number of columns to remove in order to make column unique. (raw key error)
   * @return raw key error
   */
  public int getRawKeyError() {
    if (rawKeyError == -1) {
      rawKeyError = calculateRawKeyError();
    }

    return rawKeyError;
  }

  protected int calculateRawKeyError() {
    int sumClusterSize = 0;

    for (IntArrayList cluster : clusters) {
      sumClusterSize += cluster.size();
    }

    return sumClusterSize - clusters.size();
  }

  @Override
  public String toString() {
    return "PositionListIndex{" +
      "clusters=" + clusters +
      ", rawKeyError=" + getRawKeyError() +
      '}';
  }

}
