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

package io.dingodb.calcite.executor;

import io.dingodb.cluster.ClusterService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowExecutorsExecutor extends QueryExecutor {

    private ClusterService clusterService;

    public ShowExecutorsExecutor() {
        clusterService = ClusterService.getDefault();
    }

    @Override
    Iterator<Object[]> getIterator() {
        return clusterService.getExecutors().stream()
            .map(e -> new Object[] {e.getId(), e.getHost(), e.getPort(), e.getState()})
            .iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("id");
        columns.add("host");
        columns.add("port");
        columns.add("state");
        return columns;
    }
}
