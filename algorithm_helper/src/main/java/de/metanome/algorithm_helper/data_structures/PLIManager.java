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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Manages plis and performs intersect operations.
 *
 * TODO(zwiener): Extend docs
 *
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  // TODO(zwiener): Make thread pool size accessible from the outside.
  public static transient ExecutorService exec = Executors.newFixedThreadPool(3);

  protected Map<ColumnCombinationBitset, PositionListIndex> plis;
  protected ColumnCombinationBitset allColumnCombination;
  protected SubSetGraph pliGraph;

  /**
   * TODO docs
   */
  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> plis) {
    this.plis = plis;
    pliGraph = new SubSetGraph();
    pliGraph.addAll(plis.keySet());
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

    PositionListIndex exactPli = plis.get(columnCombination);
    if (exactPli != null) {
      return exactPli;
    }

    List<ColumnCombinationBitset> subsets = pliGraph.getExistingSubsets(columnCombination);

    // Calculate set cover.
    ColumnCombinationBitset unionSoFar = new ColumnCombinationBitset();
    ColumnCombinationBitset bestSubset = new ColumnCombinationBitset();
    ColumnCombinationBitset bestUnion = new ColumnCombinationBitset();
    List<ColumnCombinationBitset> solution = new LinkedList<>();
    while (!unionSoFar.equals(columnCombination)) {
      for (ColumnCombinationBitset subset : subsets) {

        ColumnCombinationBitset currentUnion = unionSoFar.union(subset);

        if (currentUnion.size() > bestUnion.size()) {
          bestSubset = subset;
          bestUnion = currentUnion;
        }
        else if (currentUnion.size() == bestUnion.size()) {
          // If multiple subsets add the same number of columns use key error as tiebreaker.
          if (plis.get(bestSubset).getRawKeyError() > plis.get(subset).getRawKeyError()) {
            bestSubset = subset;
            bestUnion = currentUnion;
          }
        }
      }
      solution.add(bestSubset);
      unionSoFar = unionSoFar.union(bestSubset);
    }

    // Perform the necessary intersections.
    PriorityQueue<ColumnCombinationBitset> pq = new PriorityQueue<>(
        solution.size(),
        new Comparator<ColumnCombinationBitset>() {
          @Override
          public int compare(final ColumnCombinationBitset o1, final ColumnCombinationBitset o2) {
            return plis.get(o1).getRawKeyError() - plis.get(o2).getRawKeyError();
          }
        });
    
    pq.addAll(solution);

    ColumnCombinationBitset unionCombination = pq.poll();
    PositionListIndex intersect = plis.get(unionCombination);
    while (!unionCombination.equals(columnCombination)) {
      ColumnCombinationBitset currentSubset = pq.poll();

      if (currentSubset.isSubsetOf(unionCombination)) {
        continue;
      }

      unionCombination = unionCombination.union(currentSubset);
      intersect = intersect.intersect(plis.get(currentSubset));
      plis.put(unionCombination, intersect);
      pliGraph.add(unionCombination);
    }
    
    return intersect;
  }

  public Map<ColumnCombinationBitset, PositionListIndex> getPlis(ColumnCombinationBitset... columnCombinations) {

    final List<Future<?>> tasks = new LinkedList<>();

    final ConcurrentMap<ColumnCombinationBitset, PositionListIndex> pliMap = new ConcurrentHashMap<>();

    for (final ColumnCombinationBitset columnCombination : columnCombinations) {

      tasks.add(exec.submit(new Runnable() {
        @Override
        public void run() {
          PositionListIndex pli = null;
          try {
            pli = buildPli(columnCombination);
          }
          catch (PLIBuildingException e) {
            e.printStackTrace();
          }

          pliMap.put(columnCombination, pli);
        }
      }));
    }

    for (Future<?> task : tasks) {
      try {
        task.get();
      }
      catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    return pliMap;
  }
    

}
