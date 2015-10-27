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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Position list indices (or stripped partitions) are an index structure that stores the positions
 * of equal values in a nested list. A column with the values a, a, b, c, b, c transfers to the
 * position list index ((0, 1), (2, 4), (3, 5)). Clusters of size 1 are discarded. A position list
 * index should be created using the {@link PLIBuilder}.
 */
public class PositionListIndex implements Serializable {

  public static final transient int SINGLETON_VALUE = 0;
  private static final long serialVersionUID = 2;
  public static transient ExecutorService
      exec =
      getThreadPoolExecutor();
  protected List<IntArrayList> clusters;
  protected int numberOfRows;
  protected int sumClusterSize = -1;
  public PositionListIndex(List<IntArrayList> clusters, int numberOfRows) {
    this.clusters = clusters;
    this.numberOfRows = numberOfRows;
  }

  /**
   * Constructs an empty {@link PositionListIndex}.
   */
  public PositionListIndex() {
    this.clusters = new ArrayList<>();
    this.numberOfRows = 0;
  }

  protected static ThreadPoolExecutor getThreadPoolExecutor() {
    final int corePoolSize = Runtime.getRuntime()
        .availableProcessors();
    final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(corePoolSize,
                                                                         corePoolSize,
                                                                         10L, TimeUnit.SECONDS,
                                                                         new LinkedBlockingQueue<Runnable>());
    threadPoolExecutor.allowCoreThreadTimeOut(true);
    return threadPoolExecutor;
  }

  /**
   * Shuts the thread pool down.
   */
  public static void shutdownThreadPool() {
    exec.shutdown();
  }

  /**
   * Intersects the given PositionListIndex with this PositionListIndex returning a new
   * PositionListIndex. For the intersection the smaller PositionListIndex is converted into a
   * HashMap.
   * @param otherPLI the other {@link PositionListIndex} to intersect
   * @return the intersected {@link PositionListIndex}
   */
  public PositionListIndex intersect(PositionListIndex otherPLI) throws PLIBuildingException {
    // TODO(zwiener): Check that aborting operation on unique plis actually lowers execution times.
    if ((this.isUnique()) || (otherPLI.isUnique())) {
      return new PositionListIndex(new ArrayList<IntArrayList>(), getNumberOfRows());
    }

    // In most cases probing is harder than materialization. The smaller pli should be iterated for probing.
    if (this.getRawKeyError() > otherPLI.getRawKeyError()) {
      return calculateIntersection(otherPLI);
    }
    else {
      return otherPLI.calculateIntersection(this);
    }
  }

  public List<IntArrayList> getClusters() {
    return clusters;
  }

  public int getNumberOfRows() {
    return numberOfRows;
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

    PositionListIndex clone = new PositionListIndex(newClusters, this.numberOfRows);
    clone.sumClusterSize = this.sumClusterSize;
    return clone;
  }

  @Override
  public int hashCode() {
    final int prime = 31;

    List<IntOpenHashSet> setCluster = convertClustersToSets(getClusters());

    Collections.sort(setCluster, new Comparator<IntSet>() {

      @Override
      public int compare(IntSet o1, IntSet o2) {
        return o1.hashCode() - o2.hashCode();
      }
    });
    return prime * setCluster.hashCode() + getNumberOfRows();
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
    if (getNumberOfRows() != other.getNumberOfRows()) {
      return false;
    }
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
  protected PositionListIndex calculateIntersection(PositionListIndex otherPLI)
      throws PLIBuildingException {
    int[] materializedPLI = this.asArray();
    Map<IntPair, IntArrayList> map = new HashMap<>();
    buildMap(otherPLI, materializedPLI, map);

    List<IntArrayList> clusters = new ArrayList<>();
    for (IntArrayList cluster : map.values()) {
      if (cluster.size() < 2) {
        continue;
      }
      clusters.add(cluster);
    }
    return new PositionListIndex(clusters, numberOfRows);
  }

  protected void buildMap(final PositionListIndex otherPLI, final int[] materializedPLI,
                          final Map<IntPair, IntArrayList> map)
      throws PLIBuildingException
  {
    int uniqueValueCount = 0;

    List<Future<Map<IntPair, IntArrayList>>> tasks = new LinkedList<>();

    // System.out.println(otherPLI.clusters.size());
    for (final IntArrayList sameValues : otherPLI.clusters) {
      final int finalUniqueValueCount = uniqueValueCount;
      tasks.add(exec.submit(new Callable<Map<IntPair, IntArrayList>>() {
        @Override
        public Map<IntPair, IntArrayList> call() {
          final Map<IntPair, IntArrayList> internalMap = new HashMap<>();
          // System.out.println(sameValues.size());
          for (int rowCount : sameValues) {
            if ((materializedPLI.length > rowCount) &&
                (materializedPLI[rowCount] != SINGLETON_VALUE)) {
              IntPair pair = new IntPair(finalUniqueValueCount, materializedPLI[rowCount]);

              updateMap(internalMap, rowCount, pair);
            }
          }

          return internalMap;
        }
      }));
      uniqueValueCount++;
    }
    for (Future<Map<IntPair, IntArrayList>> task : tasks) {
      try {
        map.putAll(task.get());
      } catch (InterruptedException e) {
        throw new PLIBuildingException("PLI generation was interrupted.", e);
      } catch (ExecutionException e) {
        throw (PLIBuildingException) e.getCause();
      }
    }
  }

  protected void updateMap(Map<IntPair, IntArrayList> map, int rowCount,
                           IntPair pair)
  {
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
   * Materializes the PLI to an int array of row value representatives. The position list index ((0, 1),
   * (2, 4), (3, 5)) would be represented by [1, 1, 2, 3, 2, 3].
   * @return the pli as list
   */
  public int[] asArray() throws PLIBuildingException {
    final int[] materializedPli = new int[getNumberOfRows()];
    int uniqueValueCount = SINGLETON_VALUE + 1;

    List<Future<?>> tasks = new LinkedList<>();

    for (final IntArrayList sameValues : clusters) {
      final int finalUniqueValueCount = uniqueValueCount;
      tasks.add(exec.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          for (int rowIndex : sameValues) {
            materializedPli[rowIndex] = finalUniqueValueCount;
          }

          return null;
        }
      }));
      uniqueValueCount++;
    }
    for (Future<?> task : tasks) {
      try {
        task.get();
      } catch (InterruptedException e) {
        throw new PLIBuildingException("PLI materialization was interrupted.", e);
      } catch (ExecutionException e) {
        throw (PLIBuildingException) e.getCause();
      }
    }

    return materializedPli;
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
    return getSumClusterSize() - clusters.size();
  }

  public int getSumClusterSize() {
    if (sumClusterSize == -1) {
      sumClusterSize = sumClusterSize();
    }

    return sumClusterSize;
  }

  protected int sumClusterSize() {
    int sumClusterSize = 0;
    for (IntArrayList cluster : clusters) {
      sumClusterSize += cluster.size();
    }
    return sumClusterSize;
  }

  @Override
  public String toString() {
    return "PositionListIndex{" +
      "clusters=" + clusters +
      ", numberOfRows=" + getNumberOfRows() +
      ", rawKeyError=" + getRawKeyError() +
      '}';
  }

}
