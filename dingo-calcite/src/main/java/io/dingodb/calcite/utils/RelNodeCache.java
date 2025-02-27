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

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;

import java.util.Map;

@Slf4j
public final class RelNodeCache {
    private static final Map<String, RelNode> relNodeCache = new SimpleLRUMap(100);

    private RelNodeCache() {
    }

    public static RelNode getDdlRelNode(String schema, String r) {
        try {
            return relNodeCache.get(r);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    public static void setDdlRelNode(String schema, String sql, RelNode relNode) {
        try {
            relNodeCache.put(sql, relNode);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
