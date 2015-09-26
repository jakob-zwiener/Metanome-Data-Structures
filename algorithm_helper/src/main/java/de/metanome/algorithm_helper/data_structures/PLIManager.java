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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Manages plis and performs intersect operations.
 *
 * TODO(zwiener): Extend docs
 *
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager implements AutoCloseable {

  protected transient ExecutorService exec;
  protected LoadingCache<ColumnCombinationBitset, PositionListIndex> plis;
  protected PositionListIndex[] basePlis;
  protected ColumnCombinationBitset allColumnCombination;
  protected SetTrie pliSetTrie;

  /**
   * TODO docs
   *
   * FIXME(zwiener): It should be checked, that the one column plis are added to the manager.
   */
  public PLIManager(final PositionListIndex[] basePlis,
                    final int numberOfColumns,
                    final Map<ColumnCombinationBitset, PositionListIndex> cachedPlis)
      throws ColumnIndexOutOfBoundsException {

    int[] allColumnIndices = new int[numberOfColumns];
    for (int i = 0; i < numberOfColumns; i++) {
      allColumnIndices[i] = i;
    }
    allColumnCombination = new ColumnCombinationBitset(allColumnIndices);

    this.plis = CacheBuilder.newBuilder()
        // TODO(zwiener): Expose the cache size paramemter.
        .maximumSize(20)
        .removalListener(new RemovalListener<ColumnCombinationBitset, PositionListIndex>() {
          @Override
          public void onRemoval(
              final RemovalNotification<ColumnCombinationBitset, PositionListIndex> notification) {
            if (notification.getCause() != RemovalCause.REPLACED) {
              // Do not remove the one column combinations.
              if (notification.getKey().size() > 1) {
                pliSetTrie.remove(notification.getKey());
              }
            }
          }
        })
        .build(new CacheLoader<ColumnCombinationBitset, PositionListIndex>() {
          @Override
          public PositionListIndex load(final ColumnCombinationBitset key) throws Exception {
            return buildPli(key);
          }
        });

    this.basePlis = basePlis;

    this.plis.putAll(cachedPlis);

    pliSetTrie = new SetTrie(numberOfColumns);
    pliSetTrie.addAll(allColumnCombination.getContainedOneColumnCombinations());
    pliSetTrie.addAll(cachedPlis.keySet());

    exec = getThreadPoolExecutor();
  }

  /**
   * TODO: docs
   *
   * @param basePlis
   * @param numberOfColumns
   * @throws ColumnIndexOutOfBoundsException
   */
  public PLIManager(final PositionListIndex[] basePlis,
                    final int numberOfColumns) throws ColumnIndexOutOfBoundsException {
    this(basePlis, numberOfColumns, new HashMap<ColumnCombinationBitset, PositionListIndex>());
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

    // TODO(zwiener): The following 3 lines can be removed.
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
    try {
      return plis.get(columnCombination);
    } catch (ExecutionException e) {
      throw (PLIBuildingException) e.getCause();
    }
  }

  /**
   * TODO docs
   *
   * FIXME(zwiener): Check that the requested and intermediate plis are added to the cache and the
   * trie.
   */
  protected PositionListIndex buildPli(ColumnCombinationBitset columnCombination)
      throws PLIBuildingException {
    if (!columnCombination.isSubsetOf(allColumnCombination)) {
      throw new PLIBuildingException(
          "The column combination should only contain column indices of plis that are known to the pli manager.");
    }

    PositionListIndex exactPli = lookupPli(columnCombination);
    if (exactPli != null) {
      return exactPli;
    }

    List<ColumnCombinationBitset> subsets = pliSetTrie.getExistingSubsets(columnCombination);

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
        } else if (currentUnion.size() == bestUnion.size()) {
          // If multiple subsets add the same number of columns use key error as tiebreaker.
          if (lookupPli(bestSubset).getRawKeyError() > lookupPli(subset).getRawKeyError()) {
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
            return lookupPli(o1).getRawKeyError() - lookupPli(o2).getRawKeyError();
          }
        });

    pq.addAll(solution);

    ColumnCombinationBitset unionCombination = pq.poll();
    PositionListIndex intersect = lookupPli(unionCombination);
    while (!unionCombination.equals(columnCombination)) {
      ColumnCombinationBitset currentSubset = pq.poll();

      if (currentSubset.isSubsetOf(unionCombination)) {
        continue;
      }

      unionCombination = unionCombination.union(currentSubset);
      intersect = intersect.intersect(lookupPli(currentSubset));
      try {
        // TODO(zwiener): Is it a good idea to add intermediate plis to the cache?
        addPliToCache(unionCombination, intersect);
      } catch (ColumnIndexOutOfBoundsException e) {
        throw new PLIBuildingException("The pli could not be added to the pli cache.", e);
      }
    }

    return intersect;
  }

  public Map<ColumnCombinationBitset, PositionListIndex> getPlis(
      ColumnCombinationBitset... columnCombinations)
      throws PLIBuildingException {

    final List<Future<?>> tasks = new LinkedList<>();

    final ConcurrentMap<ColumnCombinationBitset, PositionListIndex>
        pliMap =
        new ConcurrentHashMap<>();

    for (final ColumnCombinationBitset columnCombination : columnCombinations) {

      tasks.add(exec.submit(new Callable<Void>() {
        @Override
        public Void call() throws PLIBuildingException, ColumnIndexOutOfBoundsException {
          PositionListIndex pli;
          pli = getPli(columnCombination);

          pliMap.put(columnCombination, pli);

          return null;
        }
      }));
    }

    for (Future<?> task : tasks) {
      try {
        task.get();
      } catch (InterruptedException e) {
        throw new PLIBuildingException("PLI generation was interrupted.", e);
      } catch (ExecutionException e) {
        throw (PLIBuildingException) e.getCause();
      }
    }

    return pliMap;
  }

  /**
   * TODO(zwiener): docs
   */
  protected PositionListIndex lookupPli(ColumnCombinationBitset columnCombination) {
    if (columnCombination.size() == 1) {
      return basePlis[columnCombination.getSetBits().get(0)];
    } else {
      return plis.getIfPresent(columnCombination);
    }
  }

  protected void addPliToCache(final ColumnCombinationBitset columnCombination,
                               final PositionListIndex intersect)
      throws ColumnIndexOutOfBoundsException {
    plis.put(columnCombination, intersect);
    pliSetTrie.add(columnCombination);

    List<ColumnCombinationBitset> supersets = pliSetTrie.getExistingSupersets(columnCombination);

    for (ColumnCombinationBitset superset : supersets) {
      if (!superset.equals(columnCombination)) {
        removePliFromCache(superset);
      }
    }
  }

  protected void removePliFromCache(final ColumnCombinationBitset columnCombination) {
    plis.invalidate(columnCombination);
    pliSetTrie.remove(columnCombination);
  }

  @Override
  public void close() {
    exec.shutdown();
  }
}
