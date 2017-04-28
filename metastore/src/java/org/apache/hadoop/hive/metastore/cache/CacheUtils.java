/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.cache.CachedStore.PartitionWrapper;
import org.apache.hadoop.hive.metastore.cache.CachedStore.TableWrapper;
import org.apache.hive.common.util.HiveStringUtils;

public class CacheUtils {
  private static final String delimit = "\t";

  public static String buildKey(String dbName, String tableName) {
    return dbName + delimit + tableName;
  }

  public static String buildKey(String dbName, String tableName, List<String> vals) {
    String key = buildKey(dbName, tableName);
    if (vals == null || vals.size() == 0) {
      return key;
    }
    for (int i=0;i<vals.size();i++) {
      key+=vals.get(i);
      if (i!=vals.size()-1) {
        key+=delimit;
      }
    }
    return key;
  }

  public static String buildKey(String dbName, String tableName, List<String> vals, String colName) {
    String key = buildKey(dbName, tableName, vals);
    return key + delimit + colName;
  }

  public static Table assemble(TableWrapper wrapper) {
    Table t = wrapper.getTable().deepCopy();
    if (wrapper.getSdHash()!=null) {
      StorageDescriptor sdCopy = SharedCache.getSdFromCache(wrapper.getSdHash()).deepCopy();
      if (sdCopy.getBucketCols()==null) {
        sdCopy.setBucketCols(new ArrayList<String>());
      }
      if (sdCopy.getSortCols()==null) {
        sdCopy.setSortCols(new ArrayList<Order>());
      }
      if (sdCopy.getSkewedInfo()==null) {
        sdCopy.setSkewedInfo(new SkewedInfo(new ArrayList<String>(),
            new ArrayList<List<String>>(), new HashMap<List<String>,String>()));
      }
      sdCopy.setLocation(wrapper.getLocation());
      sdCopy.setParameters(wrapper.getParameters());
      t.setSd(sdCopy);
    }
    return t;
  }

  public static Partition assemble(PartitionWrapper wrapper) {
    Partition p = wrapper.getPartition().deepCopy();
    if (wrapper.getSdHash()!=null) {
      StorageDescriptor sdCopy = SharedCache.getSdFromCache(wrapper.getSdHash()).deepCopy();
      if (sdCopy.getBucketCols()==null) {
        sdCopy.setBucketCols(new ArrayList<String>());
      }
      if (sdCopy.getSortCols()==null) {
        sdCopy.setSortCols(new ArrayList<Order>());
      }
      if (sdCopy.getSkewedInfo()==null) {
        sdCopy.setSkewedInfo(new SkewedInfo(new ArrayList<String>(),
            new ArrayList<List<String>>(), new HashMap<List<String>,String>()));
      }
      sdCopy.setLocation(wrapper.getLocation());
      sdCopy.setParameters(wrapper.getParameters());
      p.setSd(sdCopy);
    }
    return p;
  }

  public static boolean matches(String name, String pattern) {
    if (pattern == null || pattern.equals("*")) {
      return true;
    }
    if (Pattern.matches(pattern, HiveStringUtils.normalizeIdentifier(name))) {
      return true;
    } else {
      return false;
    }
  }
}
