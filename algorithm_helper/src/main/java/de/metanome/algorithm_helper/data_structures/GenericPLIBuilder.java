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

import java.util.List;

/**
 * @author Jakob Zwiener
 */
public interface GenericPLIBuilder {

  /**
   * Builds a {@link PositionListIndex} for every column in the input.
   * @return list of plis for all columns
   * @throws PLIBuildingException if the plis or one of the plis cannot be constructed
   */
  List<PositionListIndex> getPLIList() throws PLIBuildingException;

}
