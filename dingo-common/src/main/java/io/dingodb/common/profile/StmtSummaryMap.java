/*
 * Copyright 2021 DataCanvas
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

package io.dingodb.common.profile;

import com.codahale.metrics.CachedGauge;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class StmtSummaryMap {
    public static BlockingQueue<SqlProfile> profileQueue;
    private static final LoadingCache<String, StmtSummary> stmtSummaryMap;
    private static final BlockingQueue<AnalyzeEvent> analyzeQueue;

    private StmtSummaryMap() {
    }

    public static Iterator<Object[]> iterator() {
        return stmtSummaryMap.asMap()
        .values()
        .stream()
        .map(StmtSummary::getTuple)
        .iterator();
    }

    static {
        stmtSummaryMap = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .build(new CacheLoader<String, StmtSummary>() {
                @Override
                public @NonNull StmtSummary load(@NonNull String summaryKey) {
                    return new StmtSummary(summaryKey);
                }
            });
        profileQueue = new LinkedBlockingDeque<>(10000);
        Executors.execute("stmtSummary", StmtSummaryMap::handleProfile);
        analyzeQueue = new LinkedBlockingDeque<>(2000);
        DingoMetrics.metricRegistry.register("profileQueue", new CachedGauge<Integer>(1, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return profileQueue.size();
            }
        });

        DingoMetrics.metricRegistry.register("analyzeTaskQueue", new CachedGauge<Integer>(1, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return analyzeQueue.size();
            }
        });
    }

    private static void handleProfile() {
        while (true) {
            try {
                SqlProfile profile = profileQueue.take();
                summary(profile);
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
            }
        }
    }

    private static void summary(SqlProfile sqlProfile) {
        try {
            StmtSummary stmtSummary = stmtSummaryMap.get(sqlProfile.summaryKey());
            stmtSummary.addSqlProfile(sqlProfile);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    public static void addSqlProfile(SqlProfile sqlProfile, Connection connection) {
        if (sqlProfile == null || (sqlProfile.getExecProfile() == null && "success".equals(sqlProfile.getState()))) {
            return;
        }
        addProfileQueue(sqlProfile, connection);
    }

    public static void addProfileQueue(SqlProfile sqlProfile, Connection connection) {
        boolean slowQueryEnabled = false;
        long slowQueryThreshold = 5000;
        boolean ddlInnerProfile = false;
        try {
            String ddlProfile = connection.getClientInfo("ddl_inner_profile");
            ddlInnerProfile = "on".equalsIgnoreCase(ddlProfile);
            String enable = connection.getClientInfo("sql_profile_enable");
            if ("off".equals(enable)) {
                return;
            }
            String slowQueryEnableStr = connection.getClientInfo("slow_query_enable");
            slowQueryEnabled = "on".equals(slowQueryEnableStr);
            String slowQueryThresholdStr = connection.getClientInfo("slow_query_threshold");
            if (slowQueryThresholdStr != null) {
                slowQueryThreshold = Long.parseLong(slowQueryThresholdStr);
            }
        } catch (SQLException ignored) {
            log.error(ignored.getMessage(), ignored);
        }
        sqlProfile.end();
        if ((slowQueryEnabled && sqlProfile.duration > slowQueryThreshold)
            || (ddlInnerProfile && sqlProfile.duration > 100)) {
            LogUtils.info(log, sqlProfile.dumpTree());
        }
        if (ddlInnerProfile) {
            return;
        }
        try {
            if (sqlProfile.getStatementType() != null) {
                DingoMetrics.latency(sqlProfile.getStatementType(), sqlProfile.duration);
            }
            if (!profileQueue.offer(sqlProfile)) {
                LogUtils.info(log, "profileQueue is busy.");
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            sqlProfile.clear();
        }
    }

    public static void addAnalyzeEvent(String schemaName, String tableName, long modify) {
        AnalyzeEvent analyzeEvent = new AnalyzeEvent(schemaName, tableName, modify);
        if (!analyzeQueue.offer(analyzeEvent)) {
            LogUtils.info(log, "analyzeQueue is busy.");
        }
    }

    public static AnalyzeEvent getAnalyzeEvent() {
        while (true) {
            try {
                return analyzeQueue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }

}
