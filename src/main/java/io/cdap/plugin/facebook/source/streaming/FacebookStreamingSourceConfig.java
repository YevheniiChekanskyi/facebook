/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.facebook.source.streaming;

import io.cdap.plugin.facebook.source.common.config.BaseSourceConfig;

/**
 * Provides all required configuration for reading Facebook Insights Streaming Source.
 */
public class FacebookStreamingSourceConfig extends BaseSourceConfig {

  public FacebookStreamingSourceConfig(String referenceName) {
    super(referenceName);
  }
}
