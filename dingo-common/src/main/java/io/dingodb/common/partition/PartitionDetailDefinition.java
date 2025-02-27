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

package io.dingodb.common.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.meta.SchemaState;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class PartitionDetailDefinition implements Serializable {

    private static final long serialVersionUID = -2687766895780655226L;

    @JsonProperty("partName")
    String partName = null;

    @JsonProperty("operator")
    String operator = null;

    @JsonProperty("operand")
    @EqualsAndHashCode.Include
    Object[] operand = null;

    byte[] keys;

    SchemaState schemaState;

    public PartitionDetailDefinition() {
    }

    public PartitionDetailDefinition(Object partName, String operator, Object[] operand) {
        if (partName != null) {
            this.partName = partName.toString();
        }
        this.operator = operator;
        this.operand = operand;
    }

}
