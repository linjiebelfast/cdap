/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.registry.internal.keymaker;

import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.registry.internal.pair.KeyMaker;
import co.cask.cdap.proto.Id;

/**
 * {@link KeyMaker} for {@link Id.DatasetInstance}.
 */
public class DatasetKeyMaker implements KeyMaker<Id.DatasetInstance> {
  @Override
  public MDSKey getKey(Id.DatasetInstance datasetInstance) {
    return new MDSKey.Builder()
      .add(datasetInstance.getNamespaceId())
      .add(datasetInstance.getId())
      .build();
  }

  @Override
  public void skipKey(MDSKey.Splitter splitter) {
    splitter.skipString(); // namespace
    splitter.skipString(); // dataset
  }

  @Override
  public Id.DatasetInstance getElement(MDSKey.Splitter splitter) {
    return Id.DatasetInstance.from(splitter.getString(), splitter.getString());
  }
}
