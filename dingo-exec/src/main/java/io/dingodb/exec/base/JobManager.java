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

package io.dingodb.exec.base;

import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.transaction.base.TxnPartData;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Map;

public interface JobManager {
    default Job createJob(long startTs, long jobSeqId, CommonId txnId, DingoType parasType) {
        return createJob(startTs, jobSeqId, txnId, parasType, 0, null);
    }

    Job createJob(long startTs, long jobSeqId, CommonId txnId, DingoType parasType, long maxTimeout, Boolean isSelect);

    default Job createJob(long startTs, long jobSeqId) {
        return createJob(startTs, jobSeqId, null,  null, 0, null);
    }

    Job getJob(CommonId jobId);

    void removeJob(CommonId jobId);

    @NonNull Iterator<Object[]> createIterator(@NonNull Job job, Object @Nullable [] paras);

    @NonNull Iterator<Object[]> createIterator(@NonNull Job job, Object @Nullable [] paras, long takeNextTimeout);

    @NonNull Map<TxnPartData, Boolean> getPartData(@NonNull Job job);

    void close();

    void cancel(CommonId jobId);
}
