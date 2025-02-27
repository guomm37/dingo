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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

@Getter
@JsonTypeName("pessimistic_lock_insert")
@JsonPropertyOrder({"isolationLevel", "startTs", "forUpdateTs", "lockTimeOut", "pessimisticTxn",
    "isScan", "table", "schema", "keyMapping"})
public class PessimisticLockInsertParam extends TxnPartModifyParam {

    @JsonProperty("isScan")
    private final boolean isScan;
    @JsonProperty("isDuplicateKeyUpdate")
    private final boolean isDuplicateUpdate;
    public PessimisticLockInsertParam(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("isolationLevel") int isolationLevel,
        @JsonProperty("startTs") long startTs,
        @JsonProperty("forUpdateTs") long forUpdateTs,
        @JsonProperty("pessimisticTxn") boolean pessimisticTxn,
        @JsonProperty("primaryLockKey") byte[] primaryLockKey,
        @JsonProperty("lockTimeOut") long lockTimeOut,
        @JsonProperty("isScan") boolean isScan,
        Table table,
        boolean isDuplicateUpdate
    ) {
        super(tableId, schema, keyMapping, table, pessimisticTxn,
            isolationLevel, primaryLockKey, startTs, forUpdateTs, lockTimeOut);
        this.isScan = isScan;
        this.isDuplicateUpdate = isDuplicateUpdate;
    }
    public void inc() {
        count++;
    }
}
