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

package io.dingodb.store.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.common.serial.BufImpl;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.common.RawEngine;
import io.dingodb.sdk.service.entity.common.RegionType;
import io.dingodb.sdk.service.entity.common.StorageEngine;
import io.dingodb.sdk.service.entity.common.StoreState;
import io.dingodb.sdk.service.entity.common.StoreType;
import io.dingodb.sdk.service.entity.coordinator.CreateRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.CreateRegionResponse;
import io.dingodb.sdk.service.entity.coordinator.GetStoreMapRequest;
import io.dingodb.sdk.service.entity.coordinator.GetStoreMapResponse;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionInfo;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsRequest;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsResponse;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.meta.MetaServiceApiImpl;
import io.dingodb.store.proxy.service.CodecService;
import io.dingodb.store.proxy.service.TransactionStoreInstance;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

@Slf4j
public class MetaStoreKv {
    boolean ddl;
    CommonId metaId = null;
    CommonId partId;
    static final byte namespace = (byte) 't';
    private static MetaStoreKv instance;
    private static MetaStoreKv instanceDdl;
    Set<Location> coordinators = Services.parse(Configuration.coordinators());
    // putAbsent
    StoreService preStoreService;
    MetaKvTxn preMetaKvTxn;
    PartitionService ps = PartitionService.getService(DingoPartitionServiceProvider.RANGE_FUNC_NAME);

    long statementTimeout = 50000;

    public static synchronized void init() {
        if (instance == null || instanceDdl == null) {
            instance = new MetaStoreKv(false);
            instanceDdl = new MetaStoreKv(true);
            MetaServiceApiImpl.INSTANCE.initMetaDone = true;
            LogUtils.info(log, "meta init region ready");
        }
    }

    public static synchronized MetaStoreKv getInstance() {
        if (instance == null) {
            init();
        }
        return instance;
    }

    public static synchronized MetaStoreKv getDdlInstance() {
        if (instanceDdl == null) {
            init();
        }
        return instanceDdl;
    }

    public MetaStoreKv(boolean ddl) {
        this.ddl = ddl;
        if (!ddl) {
            partId = new CommonId(CommonId.CommonType.PARTITION, 0, 0);
            long metaPartId = checkMetaRegion();
            metaId = new CommonId(CommonId.CommonType.META, 0, 0);
            preStoreService = Services.storeRegionService(coordinators, metaPartId, TransactionUtil.STORE_RETRY);
            preMetaKvTxn = new MetaKvTxn(preStoreService, partId, r -> getMetaRegionKey(), r -> getMetaRegionEndKey());
        } else {
            partId = new CommonId(CommonId.CommonType.PARTITION, 0, 3);
            long metaPartId = checkMetaRegion();
            metaId = new CommonId(CommonId.CommonType.DDL, 0, 0);
            preStoreService = Services.storeRegionService(coordinators, metaPartId, TransactionUtil.STORE_RETRY);
            preMetaKvTxn = new MetaKvTxn(preStoreService, partId, r -> getMetaRegionKey(), r -> getMetaRegionEndKey());
        }
    }

    public long checkMetaRegion() {
        CoordinatorService coordinatorService = Services.coordinatorService(coordinators);
        long startTs = TsoService.getDefault().tso();
        byte[] startKey = getMetaRegionKey();
        byte[] endKey = getMetaRegionEndKey();

        long regionId = getScanRegionId(startKey, endKey);
        if (regionId > 0) {
            return regionId;
        }

        if (!MetaServiceApiImpl.INSTANCE.isReady() || !MetaServiceApiImpl.INSTANCE.isLeader()) {
            Utils.sleep(1000);
            return checkMetaRegion();
        }
        Range range = Range.builder().startKey(startKey).endKey(endKey).build();
        String regionName = "meta";
        long schemaId = 1001;
        if (ddl) {
            regionName = "ddl";
            schemaId = 1002;
        }
        int replica = getActReplica();
        if (replica > 3) {
            replica = 3;
        }
        CreateRegionRequest createRegionRequest = CreateRegionRequest.builder()
            .regionName(regionName)
            .range(range)
            .replicaNum(replica)
            .rawEngine(RawEngine.RAW_ENG_ROCKSDB)
            .storeEngine(StorageEngine.STORE_ENG_RAFT_STORE)
            .regionType(RegionType.STORE_REGION)
            .tenantId(0)
            .schemaId(schemaId)
            .build();
        try {
            CreateRegionResponse response = coordinatorService.createRegion(startTs, createRegionRequest);
            LogUtils.info(log, "create meta region done,name:{}", regionName);
            return response.getRegionId();
        } catch (Exception e) {
            LogUtils.error(log, "create meta region error,name:" + regionName, e);
        }
        return 0;
    }

    public long getScanRegionId(byte[] start, byte[] end) {
        List<ScanRegionInfo> regionInfoList = scanRegion(start, end);
        if (regionInfoList == null || regionInfoList.isEmpty()) {
            return 0;
        }
        return regionInfoList.get(0).getRegionId();
    }

    public List<ScanRegionInfo> scanRegion(byte[] start, byte[] end) {
        long startTs = io.dingodb.tso.TsoService.getDefault().tso();
        ScanRegionsRequest request = ScanRegionsRequest.builder()
            .key(start)
            .rangeEnd(end)
            .limit(0)
            .build();
        CoordinatorService coordinatorService = Services.coordinatorService(coordinators);
        ScanRegionsResponse response = coordinatorService.scanRegions(startTs, request);
        if (response.getRegions() == null || response.getRegions().isEmpty()) {
            return new ArrayList<>();
        }
        return response.getRegions();
    }

    public byte[] mGet(byte[] key, long startTs) {
        key = getMetaDataKey(key);

        List<byte[]> keys = Collections.singletonList(key);
        StoreService storeService = getStoreService(key);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);

        List<KeyValue> keyValueList = storeInstance.getKeyValues(startTs, keys, statementTimeout);
        if (keyValueList.isEmpty()) {
            return null;
        } else {
            return keyValueList.get(0).getValue();
        }
    }

    public byte[] mGetImmediately(byte[] key, long startTs) {
        key = getMetaDataKey(key);
        List<byte[]> keys = Collections.singletonList(key);

        StoreService storeService = getStoreService(key);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        try {
            List<KeyValue> keyValueList = storeInstance.getKeyValues(startTs, keys, 1000);
            if (keyValueList.isEmpty()) {
                return null;
            } else {
                return keyValueList.get(0).getValue();
            }
        } catch (Exception e) {
            return null;
        }
    }

    public List<byte[]> mRange(byte[] start, byte[] end, long startTs) {
        start = getMetaDataKey(start);
        end = getMetaDataKey(end);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(preStoreService, null, partId);
        StoreInstance.Range range = new StoreInstance.Range(start, end, true, false);
        Iterator<KeyValue> scanIterator = storeInstance.getScanIterator(startTs, range, statementTimeout, null);
        List<byte[]> values = new ArrayList<>();
        while (scanIterator.hasNext()) {
            values.add(scanIterator.next().getValue());
        }
        return values;
    }

    public void mDel(byte[] key, long startTs) {
        key = getMetaDataKey(key);
        StoreService storeService = getStoreService(key);
        MetaKvTxn metaKvTxn = new MetaKvTxn(storeService, partId, r -> getMetaRegionKey(), r -> getMetaRegionEndKey());
        metaKvTxn.commit(key, null, Op.DELETE.getCode(), startTs);
    }

    public void mInsert(byte[] key, byte[] value, long startTs) {
        key = getMetaDataKey(key);
        StoreService storeService = getStoreService(key);
        MetaKvTxn metaKvTxn = new MetaKvTxn(storeService, partId, r -> getMetaRegionKey(), r -> getMetaRegionEndKey());
        metaKvTxn.commit(key, value, Op.PUTIFABSENT.getCode(), startTs);
    }

    public void put(byte[] key, byte[] value, long startTs) {
        key = getMetaDataKey(key);
        StoreService storeService = getStoreService(key);
        MetaKvTxn metaKvTxn = new MetaKvTxn(storeService, partId, r -> getMetaRegionKey(), r -> getMetaRegionEndKey());
        metaKvTxn.commit(key, value, Op.PUT.getCode(), startTs);
    }

    private byte[] getMetaDataKey(byte[] key) {
        byte[] bytes = new byte[9 + key.length];
        byte[] regionKey = getMetaRegionKey();
        System.arraycopy(regionKey, 0, bytes, 0, regionKey.length);
        System.arraycopy(key, 0, bytes, 9, key.length);
        return bytes;
    }

    public byte[] getMetaRegionEndKey() {
        byte[] bytes = new byte[9];
        BufImpl buf = new BufImpl(bytes);
        // skip namespace
        buf.skip(1);
        // reset id
        long part = partId.seq;
        buf.writeLong(part + 1);
        bytes[0] = namespace;
        return bytes;
    }

    public byte[] getMetaRegionKey() {
        byte[] key = new byte[9];
        CodecService.INSTANCE.setId(key, partId.seq);
        key[0] = namespace;
        return key;
    }

    public StoreService getStoreService(byte[] key) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges
            = MetaService.root().getRangeDistribution(metaId);
        CommonId commonId = ps.calcPartId(key, ranges);
        return Services.storeRegionService(
            coordinators, commonId.seq, TransactionUtil.STORE_RETRY
        );
    }

    public static int getActReplica() {
        CoordinatorService coordinatorService
            = Services.coordinatorService(Services.parse(Configuration.coordinators()));
        GetStoreMapRequest storeMapRequest = GetStoreMapRequest.builder().epoch(0).build();
        GetStoreMapResponse response = coordinatorService.getStoreMap(
            System.identityHashCode(storeMapRequest), storeMapRequest
        );
        if (response.getStoremap() == null) {
            return 3;
        }
        long storeCount = response.getStoremap().getStores()
            .stream()
            .filter(store -> (store.getStoreType() == null || store.getStoreType() == StoreType.NODE_TYPE_STORE)
                && store.getState() == StoreState.STORE_NORMAL)
            .count();
        return (int) storeCount;
    }

}
