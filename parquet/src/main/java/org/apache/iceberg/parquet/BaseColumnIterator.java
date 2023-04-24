/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.parquet;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReader;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BaseColumnIterator {
  protected final ColumnDescriptor desc;

  // state reset for each row group
  protected PageReader pageSource = null;
  protected long triplesCount = 0L;
  protected long triplesRead = 0L;
  protected long advanceNextPageCount = 0L;
  protected Dictionary dictionary;

  private static final ExecutorService fetchPageService =
      MoreExecutors.getExitingExecutorService(
          (ThreadPoolExecutor)
              Executors.newFixedThreadPool(
                  4,
                  new ThreadFactoryBuilder()
                      .setDaemon(true)
                      .setNameFormat("iceberg-prefetch-page-pool-%d")
                      .build()));
  private Future<ParquetDataPage> nextPageFuture;

  protected BaseColumnIterator(ColumnDescriptor descriptor) {
    this.desc = descriptor;
  }

  public void setPageSource(PageReader source) {
    this.pageSource = source;
    this.triplesCount = source.getTotalValueCount();
    this.triplesRead = 0L;
    this.advanceNextPageCount = 0L;
    BasePageIterator pageIterator = pageIterator();
    pageIterator.reset();
    dictionary = ParquetUtil.readDictionary(desc, pageSource);
    pageIterator.setDictionary(dictionary);
    fetchNextPage();
    advance();
  }

  protected abstract BasePageIterator pageIterator();

  protected void advance() {
    if (triplesRead >= advanceNextPageCount) {
      Preconditions.checkState(nextPageFuture != null);
      try {
        ParquetDataPage nextPage = nextPageFuture.get();
        if (nextPage != null) {
          pageIterator().setPage(nextPage.page, nextPage.pageBytes);
          this.advanceNextPageCount += pageIterator().currentPageCount();
        }
        fetchNextPage();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public boolean hasNext() {
    return triplesRead < triplesCount;
  }

  private void fetchNextPage() {
    this.nextPageFuture =
        fetchPageService.submit(
            () -> {
              int pageValueCount = 0;
              DataPage page = null;
              while (pageValueCount <= 0) {
                page = pageSource.readPage();
                // if this is the last page then return
                if (page == null) {
                  return null;
                }
                pageValueCount = page.getValueCount();
              }
              if (page != null) {
                ByteBufferInputStream in;
                if (page instanceof DataPageV1) {
                  in = ((DataPageV1) page).getBytes().toInputStream();
                } else if (page instanceof DataPageV2) {
                  in = ((DataPageV2) page).getData().toInputStream();
                } else {
                  throw new IllegalStateException(
                      "Not able to read parquet data page of type " + page.getClass().getName());
                }
                return new ParquetDataPage(page, in);
              }
              return null;
            });
  }

  private static class ParquetDataPage {
    private final DataPage page;
    private final ByteBufferInputStream pageBytes;

    private ParquetDataPage(DataPage page, ByteBufferInputStream pageBytes) {
      this.page = page;
      this.pageBytes = pageBytes;
    }
  }
}
