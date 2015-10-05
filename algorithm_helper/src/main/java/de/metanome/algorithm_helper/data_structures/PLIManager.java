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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  public static int CACHE_SIZE = 20;
  public LoadingCache<ColumnCombinationBitset, PositionListIndex> plis;
  protected Map<ColumnCombinationBitset, PositionListIndex> basePlis;
  protected ColumnCombinationBitset allColumnCombination;
  protected SubSetGraph pliGraph;

  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> basePlis) {

    this.plis = CacheBuilder.newBuilder()
      .maximumSize(CACHE_SIZE)
      .removalListener(new RemovalListener<ColumnCombinationBitset, PositionListIndex>() {
        @Override
        public void onRemoval(final RemovalNotification<ColumnCombinationBitset, PositionListIndex> notification) {
          if (notification.getCause() != RemovalCause.REPLACED) {
            // System.out.println("removed: " + notification.getKey() + " " + notification.getCause());
            // Do not remove the one column combinations.
            if (notification.getKey().size() > 1) {
              pliGraph.remove(notification.getKey());
            }
          }
        }
      })
      .build(new CacheLoader<ColumnCombinationBitset, PositionListIndex>() {
        @Override public PositionListIndex load(final ColumnCombinationBitset key) throws Exception {
          return buildPli(key);
        }
      });

    this.plis.putAll(basePlis);

    this.basePlis = basePlis;

    pliGraph = new SubSetGraph();
    pliGraph.addAll(basePlis.keySet());

    // TODO(zwiener): Number of columns should be independent of pli map.
    int[] allColumnIndices = new int[basePlis.size()];
    for (int i = 0; i < basePlis.size(); i++) {
      allColumnIndices[i] = i;
    }
    allColumnCombination = new ColumnCombinationBitset(allColumnIndices);
  }

  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> basePlis,
                    LoadingCache<ColumnCombinationBitset, PositionListIndex> cachedPlis)
  {
    this(basePlis);

    this.plis = cachedPlis;
  }

  public PositionListIndex getPli(ColumnCombinationBitset columnCombination) throws ExecutionException {
    return plis.get(columnCombination);
  }

  public PositionListIndex getPli(ColumnCombinationBitset left, ColumnCombinationBitset right)
    throws ExecutionException
  {
    // TODO(zwiener): Union could be checked.
    return plis.get(left).intersect(plis.get(right));
  }

  protected PositionListIndex buildPli(final ColumnCombinationBitset columnCombination)
    throws PLIBuildingException, ExecutionException
  {
    if (!columnCombination.isSubsetOf(allColumnCombination)) {
      throw new PLIBuildingException(
        "The column combination should only contain column indices of plis that are known to the pli manager.");
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
          if (getPliInternal(bestSubset).getRawKeyError() > getPliInternal(subset).getRawKeyError()) {
            bestSubset = subset;
            bestUnion = currentUnion;
          }
        }
      }
      solution.add(bestSubset);
      unionSoFar = unionSoFar.union(bestSubset);
    }

    // Perform the necessary intersections.
    PositionListIndex intersect = null;
    ColumnCombinationBitset unionCombination = null;
    for (ColumnCombinationBitset subsetCombination : solution) {
      if (intersect == null) {
        intersect = getPliInternal(subsetCombination);
        unionCombination = subsetCombination;
        continue;
      }

      intersect = intersect.intersect(getPliInternal(subsetCombination));
      unionCombination = unionCombination.union(subsetCombination);
      plis.put(unionCombination, intersect);
      pliGraph.add(unionCombination);
    }

    return intersect;
  }

  protected PositionListIndex getPliInternal(ColumnCombinationBitset columnCombination) {
    if (columnCombination.size() <= 1) {
      return basePlis.get(columnCombination);
    }

    return plis.getIfPresent(columnCombination);
  }


}
