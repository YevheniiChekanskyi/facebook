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

import com.facebook.ads.sdk.APINodeList;
import com.facebook.ads.sdk.AdsInsights;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.facebook.source.common.AdsInsightsTransformer;
import io.cdap.plugin.facebook.source.common.requests.InsightsRequest;
import io.cdap.plugin.facebook.source.common.requests.InsightsRequestFactory;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Receiver that periodically pull Facebook Insights api for fresh data.
 */
public class FacebookReceiver extends Receiver<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(FacebookReceiver.class);

  private FacebookStreamingSourceConfig config;

  public FacebookReceiver(FacebookStreamingSourceConfig config) {
    super(StorageLevel.MEMORY_ONLY());
    this.config = config;
  }

  @Override
  public void onStart() {
    new Thread(() -> {
      while (!isStopped()) {
        try {
          InsightsRequest request = InsightsRequestFactory.createRequest(config);
          APINodeList<AdsInsights> currentPage = request.execute();
          while (currentPage != null) {
            fetchPage(currentPage);
            currentPage = currentPage.nextPage();
          }
          TimeUnit.MINUTES.sleep(this.config.getPollInterval());
        } catch (Exception e) {
          LOG.error("Failed to retrieve Facebook Insights", e);
          throw new RuntimeException(e);
        }
      }
    }).start();
  }

  private void fetchPage(APINodeList<AdsInsights> page) {
    page.iterator().forEachRemaining(insight -> {
      store(AdsInsightsTransformer.transform(insight, config.getSchema()));
    });
  }

  @Override
  public void onStop() {
    // noop
  }
}
