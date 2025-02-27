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

package io.dingodb.exec.operator;

import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.exec.utils.TxnMergedIterator;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public abstract class TxnScanOperatorBase extends ScanOperatorBase {
    private static @Nullable KeyValue getNextValue(@NonNull Iterator<KeyValue> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    protected static @NonNull Iterator<KeyValue> createLocalIterator(
        @NonNull CommonId txnId,
        @NonNull CommonId tableId,
        @NonNull RangeDistribution distribution
    ) {
        byte[] startKey = distribution.getStartKey();
        byte[] endKey = distribution.getEndKey();
        boolean includeStart = distribution.isWithStart();
        boolean includeEnd = distribution.isWithEnd();
        CommonId partId = distribution.getId();
        CodecService.getDefault().setId(startKey, partId.domain);
        CodecService.getDefault().setId(endKey, partId.domain);
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        byte[] encodeStart = ByteUtils.encode(CommonId.CommonType.TXN_CACHE_DATA, startKey, Op.NONE.getCode(),
            (txnIdByte.length + tableIdByte.length + partIdByte.length), txnIdByte, tableIdByte, partIdByte);
        byte[] encodeEnd = ByteUtils.encode(CommonId.CommonType.TXN_CACHE_DATA, endKey, Op.NONE.getCode(),
            (txnIdByte.length + tableIdByte.length + partIdByte.length), txnIdByte, tableIdByte, partIdByte);
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        return Iterators.transform(
            localStore.scan(new StoreInstance.Range(encodeStart, encodeEnd, includeStart, includeEnd)),
            wrap(ByteUtils::mapping)::apply);
    }

    public static @NonNull Iterator<KeyValue> createStoreIterator(
        @NonNull CommonId tableId,
        @NonNull RangeDistribution distribution,
        long scanTs,
        long timeOut
    ) {
        byte[] startKey = distribution.getStartKey();
        byte[] endKey = distribution.getEndKey();
        boolean includeStart = distribution.isWithStart();
        boolean includeEnd = distribution.isWithEnd();
        CommonId partId = distribution.getId();
        CodecService.getDefault().setId(startKey, partId.domain);
        CodecService.getDefault().setId(endKey, partId.domain);
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
        return kvStore.txnScan(
            scanTs,
            new StoreInstance.Range(startKey, endKey, includeStart, includeEnd),
            timeOut
        );
    }

    protected static @NonNull Iterator<Object[]> createMergedIterator(
        Iterator<KeyValue> localKVIterator,
        Iterator<KeyValue> kvKVIterator,
        KeyValueCodec decoder
    ) {
        return new TxnMergedIterator(localKVIterator, kvKVIterator, decoder);
    }

}
