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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  public static final int N_THREADS = 10;
  // TODO(zwiener): Make thread pool size accessible from the outside.
  public static transient ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(
    N_THREADS));
  protected List<PositionListIndex> plis;
  protected ColumnCombinationBitset allColumnCombination;

  public PLIManager(final List<PositionListIndex> pliList) {
    plis = pliList;
    int[] allColumnIndices = new int[plis.size()];
    for (int i = 0; i < plis.size(); i++) {
      allColumnIndices[i] = i;
    }
    allColumnCombination = new ColumnCombinationBitset(allColumnIndices);
  }

  public PositionListIndex buildPli(final ColumnCombinationBitset columnCombination) throws PLIBuildingException
  {
    if (!columnCombination.isSubsetOf(allColumnCombination)) {
      throw new PLIBuildingException(
        "The column combination should only contain column indices of plis that are known to the pli manager.");
    }

    System.out.println("Start intersect");

    final MinMaxPriorityQueue<PositionListIndex>[] priorityQueue = new MinMaxPriorityQueue[] {
      MinMaxPriorityQueue.orderedBy(
        new Comparator<PositionListIndex>() {
          @Override public int compare(final PositionListIndex o1, final PositionListIndex o2) {
            return o1.getRawKeyError() - o2.getRawKeyError();
          }
        }).create()
    };
    final MinMaxPriorityQueue<PositionListIndex>[] preparationQueue = new MinMaxPriorityQueue[] {
      MinMaxPriorityQueue.orderedBy(
        new Comparator<PositionListIndex>() {
          @Override public int compare(final PositionListIndex o1, final PositionListIndex o2) {
            return o1.getRawKeyError() - o2.getRawKeyError();
          }
        }).create()
    };

    for (int columnIndex : columnCombination.getSetBits()) {
      preparationQueue[0].add(plis.get(columnIndex));
    }

    priorityQueue[0].add(preparationQueue[0].poll());
    priorityQueue[0].add(preparationQueue[0].poll());

    final Object prioLock = new Object();
    final Object prepLock = new Object();
    final Semaphore prioSemaphore = new Semaphore(2);

    final PositionListIndex[] minPli = { priorityQueue[0].peek() };
    final int[] minError = { minPli[0].getRawKeyError() };

    List<ListenableFuture<?>> tasks = new LinkedList<>();
    while (true) {
      tasks.add(exec.submit(new Runnable() {
        @Override public void run() {
          while (true) {
            if (priorityQueue[0].size() < 2) {
              break;
            }
            PositionListIndex left;
            PositionListIndex right;
            synchronized (prioLock) {
              try {
                prioSemaphore.acquire(2);
              }
              catch (InterruptedException e) {
                e.printStackTrace();
              }
              left = priorityQueue[0].poll();
              right = priorityQueue[0].poll();
            }
            PositionListIndex intersect = left.intersect(right);
            synchronized (prioLock) {
              priorityQueue[0].add(intersect);
              if (intersect.getRawKeyError() < minError[0]) {
                minError[0] = intersect.getRawKeyError();
                minPli[0] = intersect;
              }
              prioSemaphore.release();
            }
          }
        }
      }));
      for (int i = 0; i < N_THREADS; i++) {
        tasks.add(exec.submit(new Runnable() {
          @Override public void run() {
            while (true) {
              PositionListIndex left;
              PositionListIndex right;

              synchronized (prepLock) {
                if (preparationQueue[0].size() < 2) {
                  break;
                }

                left = preparationQueue[0].poll();
                // right = preparationQueue[0].poll();
              }
              PositionListIndex intersect = left.intersect(minPli[0]);
              synchronized (prioLock) {
                priorityQueue[0].add(intersect);
                prioSemaphore.release();
              }
            }
          }
        }));
      }
      try {
        for (ListenableFuture<?> task : tasks) {
          task.get();
        }
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
      catch (ExecutionException e) {
        e.printStackTrace();
      }
      tasks.clear();

      if (preparationQueue[0].size() == 1) {
        priorityQueue[0].add(preparationQueue[0].poll());
      }
      if (priorityQueue[0].size() < 2) {
        break;
      }
      if (preparationQueue[0].size() < 1) {
        MinMaxPriorityQueue<PositionListIndex> aux = priorityQueue[0];
        priorityQueue[0] = preparationQueue[0];
        preparationQueue[0] = aux;
        priorityQueue[0].add(preparationQueue[0].poll());
        priorityQueue[0].add(preparationQueue[0].poll());
        prioSemaphore.release(2);
      }
    }



    /*long sumSync = 0;

    List<ListenableFuture<PositionListIndex>> tasks = new LinkedList<>();
    while (true) {
      if (priorityQueue.size() < 2) {
        break;
      }
      // printQueue(priorityQueue);
      PositionListIndex minPli = priorityQueue.peek();
      for (int i = 0; i < N_THREADS; i++) {
        if (priorityQueue.size() < 2) {
          break;
        }

        final PositionListIndex leftPli = priorityQueue.poll();

        final PositionListIndex rightPli;
        if (minPli.getRawKeyError() < leftPli.getRawKeyError()) {
          System.out.println(String.format("exchange %d %d", minPli.getRawKeyError(), leftPli.getRawKeyError()));
          rightPli = minPli;
        }
        else {
          rightPli = priorityQueue.poll();
        }

        tasks.add(exec.submit(new Callable<PositionListIndex>() {

          @Override public PositionListIndex call() throws Exception {
            PositionListIndex intersect = leftPli.intersect(rightPli);
            System.out.println(String.format("%d, %d: %d", leftPli.getRawKeyError(), rightPli.getRawKeyError(),
              intersect.calculateRawKeyError()));
            return intersect;
          }
        }));
      }
      long beforeSync = -1;
      for (ListenableFuture<PositionListIndex> task : tasks) {
        try {
          PositionListIndex result = task.get();
          if (beforeSync == -1) {
            beforeSync = System.nanoTime();
          }
          if (result.getRawKeyError() == 0) {
            System.out.println("Hello");
          }
          priorityQueue.add(result);
        }
        catch (InterruptedException e) {
          e.printStackTrace();
        }
        catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
      final long syncElapsed = System.nanoTime() - beforeSync;
      System.out.println(String.format("Sync took: %f", syncElapsed / 1000000000d));
      sumSync += syncElapsed;
      tasks.clear();
    }

    System.out.println(String.format("All syncs took: %f", sumSync / 1000000000d));*/

    /*final Object lock1 = new Object();
    final PositionListIndex[] minPli = { priorityQueue.peek() };
    final int[] minKeyError = { minPli[0].getRawKeyError() };

    List<ListenableFuture<?>> tasks = new LinkedList<>();
    for (int i = 0; i < N_THREADS; i++) {
      tasks.add(exec.submit(new Runnable() {
        @Override public void run() {
          while (true) {
            PositionListIndex leftPli;
            PositionListIndex rightPli;
            synchronized (lock1) {
              if (priorityQueue.size() < 2) {
                break;
              }
              // printQueue(priorityQueue);
              leftPli = priorityQueue.poll();
              if (minKeyError[0] < leftPli.getRawKeyError()) {
                rightPli = minPli[0];
                System.out.println(String.format("exchange %d %d", rightPli.getRawKeyError(),
                  leftPli.getRawKeyError()));
              }
              else {
                rightPli = priorityQueue.poll();
              }
            }
            PositionListIndex intersect = leftPli.intersect(rightPli);
            System.out.println(String.format("%d, %d: %d", leftPli.getRawKeyError(), rightPli.getRawKeyError(),
              intersect.calculateRawKeyError()));
            if (intersect.getRawKeyError() < 100) {
              System.out.println("Stop");
            }
            synchronized (lock1) {
              priorityQueue.add(intersect);
              if (intersect.getRawKeyError() < minKeyError[0]) {
                minPli[0] = intersect;
                minKeyError[0] = intersect.getRawKeyError();
              }
            }
          }
        }
      }));
    }

    for (ListenableFuture<?> task : tasks) {
      try {
        task.get();
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
      catch (ExecutionException e) {
        e.printStackTrace();
      }
    }*/

    System.out.println(String.format("End intersect %d", priorityQueue[0].peek().getRawKeyError()));

    return priorityQueue[0].poll();
  }

  private void printQueue(final MinMaxPriorityQueue<PositionListIndex> priorityQueue) {
    System.out.println("queue");
    List<PositionListIndex> pliList = new ArrayList<>(priorityQueue);
    pliList.sort(new Comparator<PositionListIndex>() {
      @Override public int compare(final PositionListIndex o1, final PositionListIndex o2) {
        return o1.getRawKeyError() - o2.getRawKeyError();
      }
    });
    for (PositionListIndex pli : pliList) {
      System.out.println(pli.getRawKeyError());
    }
    System.out.println("queue");
  }


}
