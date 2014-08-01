/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.lib.table.MetaTableUtil;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.google.inject.Inject;

/**
 * Helper class for working with the dataset table used by
 * {@link com.continuuity.internal.app.runtime.schedule.DataSetBasedScheduleStore}.
 */
public class ScheduleStoreTableUtil extends MetaTableUtil {
  public static final String SCHEDULE_STORE_DATASET_NAME = "schedulestore";

  @Inject
  public ScheduleStoreTableUtil(DatasetFramework framework, CConfiguration conf) {
    super(framework, conf);
  }

  @Override
  public String getMetaTableName() {
    return SCHEDULE_STORE_DATASET_NAME;
  }
}
