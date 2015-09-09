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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.differential.IntegratedBinaryPacking;
import me.lemire.integercompression.differential.IntegratedComposition;
import me.lemire.integercompression.differential.IntegratedVariableByte;

/**
 * Position list indices (or stripped partitions) are an index structure that stores the positions
 * of equal values in a nested list. A column with the values a, a, b, c, b, c transfers to the
 * position list index ((0, 1), (2, 4), (3, 5)). Clusters of size 1 are discarded. A position list
 * index should be created using the {@link PLIBuilder}.
 */
public class PositionListIndex implements Serializable {

  public static final transient int SINGLETON_VALUE = 0;
  private static final long serialVersionUID = 6;
  // TODO(zwiener): Make thread pool size accessible from the outside.
  public static transient ExecutorService exec = Executors.newFixedThreadPool(1);
  protected static transient IntegratedComposition codec =
    new IntegratedComposition(new IntegratedBinaryPacking(), new IntegratedVariableByte());
  protected int[][] clusters;
  protected int numberOfRows;
  protected int rawKeyError = -1;
  protected int[] sizes;

  public PositionListIndex(List<IntArrayList> clusters, int numberOfRows) {
    int[][] arrayClusters = new int[clusters.size()][];

    int i = 0;
    for (IntArrayList cluster : clusters) {
      arrayClusters[i++] = cluster.toIntArray(new int[cluster.size()]);
    }
    this.clusters = compress(arrayClusters);
    this.numberOfRows = numberOfRows;
  }

  /**
   * Constructs an empty {@link PositionListIndex}.
   */
  public PositionListIndex() {
    clusters = new int[0][];
    this.numberOfRows = 0;
  }

  protected int[][] compress(int[][] clusters) {

    final int[][] compressedClusters = new int[clusters.length][];
    sizes = new int[clusters.length];

    List<Future<?>> tasks = new LinkedList<>();
    int clusterNumber = 0;
    for (final int[] cluster : clusters) {
      final int finalClusterNumber = clusterNumber;
      /*tasks.add(exec.submit(new Runnable() {
        @Override public void run() {*/
          compressedClusters[finalClusterNumber] = compressCluster(cluster);
       /*}
      }));*/
      sizes[clusterNumber] = cluster.length;
      clusterNumber++;
    }

    for (Future<?> task : tasks) {
      try {
        task.get();
      }
      catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    return compressedClusters;
  }

  private int[] compressCluster(final int[] cluster) {
    int[] compressed = new int[cluster.length + 1024];

    final IntWrapper outputOffset = new IntWrapper(0);
    codec.compress(cluster, new IntWrapper(0), cluster.length, compressed,
      outputOffset);

    return Arrays.copyOf(compressed, outputOffset.intValue());
  }

  protected int[][] uncompress(int[][] compressedClusters) {

    final int[][] clusters = new int[compressedClusters.length][];

    List<Future<?>> tasks = new LinkedList<>();
    int clusterIndex = 0;
    for (final int[] compressedCluster : compressedClusters) {
      final int finalClusterIndex = clusterIndex;
      final int size = sizes[clusterIndex];
      /*tasks.add(exec.submit(new Runnable() {
        @Override public void run() {*/
      int[] deltaEncoded = uncompressCluster(compressedCluster, size);

      clusters[finalClusterIndex] = deltaEncoded;
      /*}
  }));*/
      clusterIndex++;
    }

    for (Future<?> task : tasks) {
      try {
        task.get();
      }
      catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    return clusters;
  }

  protected int[] uncompressCluster(final int[] compressedCluster, int size) {

    int[] uncompressed = new int[size];

    codec.uncompress(compressedCluster, new IntWrapper(0), compressedCluster.length, uncompressed, new IntWrapper(0));

    return uncompressed;
  }


  /**
   * Intersects the given PositionListIndex with this PositionListIndex returning a new
   * PositionListIndex. For the intersection the smaller PositionListIndex is converted into a
   * HashMap.
   * @param otherPLI the other {@link PositionListIndex} to intersect
   * @return the intersected {@link PositionListIndex}
   */
  public PositionListIndex intersect(PositionListIndex otherPLI) {
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

  public int[][] getClusters() {
    return uncompress(clusters);
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
    PositionListIndex clone = new PositionListIndex();
    clone.numberOfRows = numberOfRows;
    clone.clusters = new int[this.clusters.length][];
    System.arraycopy(this.clusters, 0, clone.clusters, 0, this.clusters.length);
    clone.rawKeyError = this.rawKeyError;
    clone.sizes = new int[this.sizes.length];
    System.arraycopy(this.sizes, 0, clone.sizes, 0, this.sizes.length);
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
      List<IntOpenHashSet> setCluster = convertClustersToSets(this.uncompress(clusters));
      List<IntOpenHashSet> otherSetCluster = convertClustersToSets(other.uncompress(other.clusters));

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

  protected List<IntOpenHashSet> convertClustersToSets(int[][] listCluster) {
    List<IntOpenHashSet> setClusters = new LinkedList<>();
    for (int[] cluster : listCluster) {
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

  protected void buildMap(PositionListIndex otherPLI, int[] materializedPLI,
                          Map<IntPair, IntArrayList> map)
  {
    int uniqueValueCount = 0;
    for (int[] sameValues : otherPLI.uncompress(otherPLI.clusters)) {
      for (int rowCount : sameValues) {
        if ((materializedPLI.length > rowCount) &&
          (materializedPLI[rowCount] != SINGLETON_VALUE)) {
          IntPair pair = new IntPair(uniqueValueCount, materializedPLI[rowCount]);
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

  /**
   * Returns the position list index in a map representation. Every row index maps to a value
   * reconstruction. As the original values are unknown they are represented by a counter. The
   * position list index ((0, 1), (2, 4), (3, 5)) would be represented by {0=0, 1=0, 2=1, 3=2, 4=1,
   * 5=2}.
   * @return the pli as hash map
   */
  public Int2IntOpenHashMap asHashMap() {
    Int2IntOpenHashMap hashedPLI = new Int2IntOpenHashMap(clusters.length);
    int uniqueValueCount = 0;
    for (int[] sameValuesCompressed : uncompress(clusters)) {
      for (int rowIndex : sameValuesCompressed) {
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
  public int[] asArray() {
    int[] materializedPli = new int[getNumberOfRows()];
    int uniqueValueCount = SINGLETON_VALUE + 1;
    for (int[] sameValues : uncompress(clusters)) {
      for (int rowIndex : sameValues) {
        materializedPli[rowIndex] = uniqueValueCount;
      }
      uniqueValueCount++;
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
    return clusters.length;
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

    // FIXME(zwiener): Make this uncompression oboslete:
    for (int[] cluster : uncompress(clusters)) {
      sumClusterSize += cluster.length;
    }

    return sumClusterSize - clusters.length;
  }

  @Override
  public String toString() {
    return "PositionListIndex{" +
      "clusters=" + uncompress(clusters) +
      ", numberOfRows=" + getNumberOfRows() +
      ", rawKeyError=" + getRawKeyError() +
      '}';
  }

}
