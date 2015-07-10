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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

/**
 * Position list indices (or stripped partitions) are an index structure that stores the positions
 * of equal values in a nested list. A column with the values a, a, b, c, b, c transfers to the
 * position list index ((0, 1), (2, 4), (3, 5)). Clusters of size 1 are discarded. A position list
 * index should be created using the {@link PLIBuilder}.
 */
public class PositionListIndex implements Serializable {

  public static final transient int SINGLETON_VALUE = 0;
  private static final long serialVersionUID = 3;
  // TODO(zwiener): Make thread pool size accessible from the outside.
  public static transient ExecutorService exec = Executors.newFixedThreadPool(4);
  protected List<IntOpenHashSet> clusters;
  protected int numberOfRows;
  protected int rawKeyError = -1;

  public PositionListIndex(List<IntArrayList> clusters, int numberOfRows) {
    // FIXME(zwiener): Remove conversion.
    this.clusters = convertClustersToSets(clusters);
    // this.clusters = clusters;
    this.numberOfRows = numberOfRows;
  }


  /**
   * Constructs an empty {@link PositionListIndex}.
   */
  public PositionListIndex() {
    this.clusters = new ArrayList<>();
    this.numberOfRows = 0;
  }

  /**
   * Intersects the given {@link PositionListIndex}es with this {@link PositionListIndex} returning a new
   * {@link PositionListIndex}. For the intersection the all given plis are materialized.
   * @param otherPLIs the other {@link PositionListIndex}es to intersect
   * @return the intersected {@link PositionListIndex}
   */
  public PositionListIndex intersect(PositionListIndex... otherPLIs) {
    //TODO Optimize Smaller PLI as Hashmap?
    return calculateIntersection(otherPLIs);

  }

  public List<IntArrayList> getClusters() {
    // FIXME(zwiener): Remove conversion.
    return toListOfArrayLists(clusters);

    // return clusters;
  }

  protected List<IntArrayList> toListOfArrayLists(List<IntOpenHashSet> clusters) {
    List<IntArrayList> convertedClusters = new ArrayList<>();

    for (IntOpenHashSet cluster : clusters) {
      convertedClusters.add(new IntArrayList(cluster));
    }

    return convertedClusters;
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
    for (IntArrayList cluster : toListOfArrayLists(clusters)) {
      // for (IntArrayList cluster : clusters) {
      newClusters.add(cluster.clone());
    }

    PositionListIndex clone = new PositionListIndex(newClusters, this.numberOfRows);
    clone.rawKeyError = this.rawKeyError;
    return clone;
  }

  @Override
  public int hashCode() {
    final int prime = 31;

    List<IntOpenHashSet> setCluster = convertClustersToSets(getClusters());

    Collections.sort(setCluster, new Comparator<IntOpenHashSet>() {

      @Override
      public int compare(IntOpenHashSet o1, IntOpenHashSet o2) {
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
      // FIXME(zwiener): Inline variables.
      List<IntOpenHashSet> setCluster = clusters;
      List<IntOpenHashSet> otherSetCluster = other.clusters;

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
   * Intersects given {@link PositionListIndex}es with this pli and returns the outcome as a new
   * PositionListIndex.
   * @param otherPLIs the other {@link PositionListIndex}s to intersect
   * @return the intersected {@link PositionListIndex}
   */
  protected PositionListIndex calculateIntersection(final PositionListIndex... otherPLIs) {
    LinkedList<Integer>[] materializedPli = this.asLinkedListArray();

    boolean[] validRows = new boolean[getNumberOfRows()];

    for (PositionListIndex currentPLI : otherPLIs) {
      int processedClusters = 0;
      boolean[] skipSet = new boolean[getNumberOfRows()];
      for (IntOpenHashSet rightCluster : currentPLI.clusters) {
        // System.out.println(String.format("%d / %d", processedClusters++, currentPLI.clusters.size()));
        int processedClusterEntries = 0;
        for (int rowIndex : rightCluster) {
          if (processedClusterEntries++ % 1000 == 0) {
            // System.out.println(String.format("%d / %d", processedClusterEntries, rightCluster.size()));
          }
          if ((rowIndex > materializedPli.length) || (materializedPli[rowIndex] == null)) {
            continue;
          }

          // System.out.println(rowIndex);
          validRows[rowIndex] = true;
          // If the cluster has already been intersected it does not need to be intersected again.
          if (skipSet[rowIndex]) {
            continue;
          }

          LinkedList<Integer> leftIntersect = materializedPli[rowIndex];
          LinkedList<Integer> leftDifference = new LinkedList<>();
          // TODO(zwiener): Try always using arrays.
          // final int[] rightIntArray = rightCluster.toIntArray();
          removeRetainAll(leftIntersect, leftDifference, rightCluster);
          // retainAll(leftIntersect, rightIntArray);
          // removeAll(leftDifference, rightIntArray);
          if (leftDifference.size() < 2) {
            for (int rowToUpdate : leftDifference) {
              materializedPli[rowToUpdate] = null;
            }
          }
          else {
            for (int rowToUpdate : leftDifference) {
              materializedPli[rowToUpdate] = leftDifference;
            }
          }

          for (int rowIndicesToSkip : leftIntersect) {
            skipSet[rowIndicesToSkip] = true;
          }
        }
      }
      /*
      // Intersect clusters with stripped rows.
      int numberOfProcessedRows = 0;
      for (int uniqueRow = 0; uniqueRow < validRows.length; uniqueRow++) {
        // System.out.println(String.format("%d / %d", numberOfProcessedRows++, validRows.size()));
        if ((validRows[uniqueRow]) || (uniqueRow > materializedPli.length) || (materializedPli[uniqueRow] == null)) {
          continue;
        }
        materializedPli[uniqueRow].removeFirstOccurrence(uniqueRow);
        if (materializedPli[uniqueRow].size() < 2) {
          for (int rowToUpdate : materializedPli[uniqueRow]) {
            materializedPli[rowToUpdate] = null;
          }
        }
        materializedPli[uniqueRow] = null;
      }
      */
    }

    List<IntOpenHashSet> strippedDeduplicatedClusters = new ArrayList<>();
    boolean[] skipSet = new boolean[getNumberOfRows()];
    for (int rowIndex = 0; rowIndex < materializedPli.length; rowIndex++) {
      if (skipSet[rowIndex]) {
        continue;
      }

      if (!validRows[rowIndex]) {
        continue;
      }

      LinkedList<Integer> cluster = materializedPli[rowIndex];
      if ((cluster == null) || (cluster.size() < 2)) {
        continue;
      }

      for (int rowIndicesToSkip : cluster) {
        skipSet[rowIndicesToSkip] = true;
      }

      strippedDeduplicatedClusters.add(new IntOpenHashSet(cluster));
    }

    /*
    Set<IntArrayList> strippedDeduplicatedClusters = new HashSet<>();
    for (IntArrayList cluster : materializedPli) {
      if ((cluster == null) || (cluster.size() < 2)) {
        continue;
      }

      strippedDeduplicatedClusters.add(cluster);
    }
    */

    // FIXME(zwiener): remove this conversion hack.
    PositionListIndex intersectedPLI = new PositionListIndex(new ArrayList<IntArrayList>(), numberOfRows);
    intersectedPLI.clusters = strippedDeduplicatedClusters;

    return intersectedPLI;
  }

  protected void retainAll(LinkedList<Integer> left, int[] right) {

    Iterator<Integer> leftIterator = left.iterator();

    while (leftIterator.hasNext()) {
      int leftIndex = leftIterator.next();

      // for (int leftIndex : leftIterator) {

      // TODO(zwiener): search could be restricted.
      int index = Arrays.binarySearch(right, leftIndex);

      if ((index < 0) || (right[index] != leftIndex)) {
        leftIterator.remove();
      }
    }
  }

  protected void removeAll(LinkedList<Integer> left, int[] right) {
    Iterator<Integer> leftIterator = left.iterator();

    while (leftIterator.hasNext()) {
      int leftIndex = leftIterator.next();

      // TODO(zwiener): search could be restricted.
      int index = Arrays.binarySearch(right, leftIndex);

      if (index < 0) {
        continue;
      }

      if (right[index] == leftIndex) {
        leftIterator.remove();
      }
    }
  }

  protected void removeRetainAll(LinkedList<Integer> retain, LinkedList<Integer> remove, IntOpenHashSet right) {
    Iterator<Integer> retainIterator = retain.iterator();

    while (retainIterator.hasNext()) {
      int leftIndex = retainIterator.next();

      // TODO(zwiener): search could be restricted.
      // int index = Arrays.binarySearch(right, leftIndex);

      // if ((index < 0) || (right[index] != leftIndex)) {
      if (!right.contains(leftIndex)) {
        retainIterator.remove();
        remove.add(leftIndex);
      }
    }
  }

  protected void buildMap(int[][] materializedPLIs, Map<IntPair, IntArrayList> map)
  {
    int uniqueValueCount = 0;
    for (IntOpenHashSet sameValues : this.clusters) {
      for (int rowCount : sameValues) {
        int[] materializedRow = new int[materializedPLIs.length + 1];  // Extra slot for not materialized pli.
        boolean rowIsUnique = false;
        for (int i = 0; i < materializedPLIs.length; i++) {
          int[] materializedPLI = materializedPLIs[i];
          if ((materializedPLI.length <= rowCount) ||
            (materializedPLI[rowCount] == SINGLETON_VALUE)) {
            rowIsUnique = true;
            break;
          }
          materializedRow[i] = materializedPLI[rowCount];
        }
        if (!rowIsUnique) {
          materializedRow[materializedPLIs.length] = uniqueValueCount;
          updateMap(map, rowCount, new IntPair(materializedRow));
        }
      }
      uniqueValueCount++;
    }
  }

  protected void updateMap(Map<IntPair, IntArrayList> map, int rowCount, IntPair materializedRow) {
    if (map.containsKey(materializedRow)) {
      IntArrayList currentList = map.get(materializedRow);
      currentList.add(rowCount);
    }
    else {
      IntArrayList newList = new IntArrayList();
      newList.add(rowCount);
      map.put(materializedRow, newList);
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
    // for (IntArrayList sameValues : toListOfArrayLists(clusters)) {
    for (IntOpenHashSet sameValues : clusters) {
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
  public int[] asArray() {
    int[] materializedPli = new int[getNumberOfRows()];
    int uniqueValueCount = SINGLETON_VALUE + 1;
    // for (IntArrayList sameValues : toListOfArrayLists(clusters)) {
    for (IntOpenHashSet sameValues : clusters) {
      for (int rowIndex : sameValues) {
        materializedPli[rowIndex] = uniqueValueCount;
      }
      uniqueValueCount++;
    }

    return materializedPli;
  }

  public IntArrayList[] asSetArray() {
    IntArrayList[] materializedPli = new IntArrayList[getNumberOfRows()];

    for (IntOpenHashSet currentCluster : clusters) {
      for (int rowIndex : currentCluster) {
        materializedPli[rowIndex] = new IntArrayList(currentCluster);
      }
    }

    return materializedPli;
  }

  public int[] asPointerArray() {
    int[] pointerArray = new int[getNumberOfRows() + 1];

    for (IntOpenHashSet currentCluster : clusters) {
      for (int i = 0; i < currentCluster.size(); i++) {
        int nextIndex = (i + 1) % currentCluster.size();
        // pointerArray[currentCluster.get(i) + 1] = currentCluster.get(nextIndex) + 1;
      }
    }

    return pointerArray;
  }

  protected LinkedList<Integer>[] asLinkedListArray() {

    LinkedList<Integer>[] materializedPli = new LinkedList[getNumberOfRows()];

    for (IntOpenHashSet currentCluster : clusters) {
      LinkedList<Integer> clusterList = new LinkedList<>(currentCluster);
      for (int i : currentCluster) {
        materializedPli[i] = clusterList;
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
    if (rawKeyError == -1) {
      rawKeyError = calculateRawKeyError();
    }

    return rawKeyError;
  }

  protected int calculateRawKeyError() {
    int sumClusterSize = 0;

    // for (IntArrayList cluster : toListOfArrayLists(clusters)) {
    for (IntOpenHashSet cluster : clusters) {
      sumClusterSize += cluster.size();
    }

    return sumClusterSize - clusters.size();
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
