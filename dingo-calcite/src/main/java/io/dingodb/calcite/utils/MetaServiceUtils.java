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

package io.dingodb.calcite.utils;

import io.dingodb.calcite.DingoTable;
import io.dingodb.common.CommonId;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.MetaService;
import org.apache.calcite.plan.RelOptTable;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class MetaServiceUtils {

    public static final String SCHEMA_NAME = "DINGO";

    private MetaServiceUtils() {
    }

    public static @NonNull CommonId getTableId(RelOptTable table) {
        DingoTable dingoTable = table.unwrapOrThrow(DingoTable.class);
        return dingoTable.getTable().tableId;
    }

    public static @NonNull TableInfo getTableInfo(RelOptTable table) {
        MetaService metaService = MetaService.root();
        DingoTable dingoTable = table.unwrapOrThrow(DingoTable.class);
        CommonId tableId = dingoTable.getTable().tableId;
        return new TableInfo(
            tableId,
            metaService.getRangeDistribution(tableId)
        );
    }

    public static @NonNull TableInfo getTableInfo(long pointTs, RelOptTable table) {
        MetaService metaService;
        if (pointTs > 0) {
            metaService = MetaService.snapshot(pointTs);
        } else {
            metaService = MetaService.root();
        }
        DingoTable dingoTable = table.unwrapOrThrow(DingoTable.class);
        CommonId tableId = dingoTable.getTable().tableId;
        return new TableInfo(
            tableId,
            metaService.getRangeDistribution(tableId)
        );
    }

    public static String getSchemaName(String tableName) {
        if (tableName.contains("\\.")) {
            return tableName.split("\\.")[0];
        }
        return SCHEMA_NAME;
    }
}
