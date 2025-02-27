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

import io.dingodb.common.meta.Tenant;
import io.dingodb.common.mysql.util.DataTimeUtils;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.meta.InfoSchemaService;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class ShowTenantExecutor extends QueryExecutor {

    private InfoSchemaService service;

    public ShowTenantExecutor() {
        service = InfoSchemaService.root();
    }

    @Override
    Iterator<Object[]> getIterator() {
        Stream<Tenant> tenantStream;
        if (TenantConstant.TENANT_ID == 0) {
            tenantStream = service.listTenant().stream().map(o -> (Tenant) o);
        } else {
            tenantStream = service.listTenant().stream()
                .map(o -> (Tenant) o).filter(t -> t.getId() == TenantConstant.TENANT_ID);
        }
        return tenantStream
            .map(t -> new Object[] {
                t.getId(),
                t.getName().toLowerCase(),
                DataTimeUtils.getTimeStamp(new Timestamp(t.getCreatedTime())),
                DataTimeUtils.getTimeStamp(new Timestamp(t.getUpdatedTime())),
                t.isDelete(),
                t.getRemarks()})
            .iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("TENANT_ID");
        columns.add("TENANT_NAME");
        columns.add("CREATED_TIME");
        columns.add("UPDATED_TIME");
        columns.add("IS_DELETE");
        columns.add("REMARKS");
        return columns;
    }
}
