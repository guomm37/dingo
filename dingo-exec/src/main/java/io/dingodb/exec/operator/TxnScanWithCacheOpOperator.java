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

import io.dingodb.common.log.LogUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ScanWithRelOpParam;
import io.dingodb.exec.utils.RelOpUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TxnScanWithCacheOpOperator extends TxnScanWithRelOpOperatorBase {
    public static TxnScanWithCacheOpOperator INSTANCE = new TxnScanWithCacheOpOperator();

    @Override
    protected @NonNull Scanner getScanner(@NonNull Context context, @NonNull Vertex vertex) {
        ScanWithRelOpParam param = vertex.getParam();
        if (context.getDistribution().getId().seq != 80012) {
            int i = 0;
            LogUtils.debug(log, "for debug");
        }
        if (param.getCoprocessor(context.getDistribution().getId()) != null) {
            return RelOpUtils::doScan;
        }
        return RelOpUtils::doScanWithCacheOp;
    }
}
