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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Plugin reads data from Facebook Insights Api periodically fetching fresh data.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(FacebookStreamingSource.NAME)
@Description(FacebookStreamingSource.DESCRIPTION)
public class FacebookStreamingSource extends StreamingSource<StructuredRecord> {
  static final String NAME = "FacebookStreamingSource";
  static final String DESCRIPTION = "Read data from Facebook Insights API periodically fetching fresh data.";
  private FacebookStreamingSourceConfig config;

  public FacebookStreamingSource(FacebookStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    IdUtils.validateId(config.referenceName);
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
    validateConfiguration(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) {
    validateConfiguration(context.getFailureCollector());
    return context.getSparkStreamingContext().receiverStream(new FacebookReceiver(config));
  }

  private void validateConfiguration(FailureCollector failureCollector) {
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
  }
}
