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

package io.dingodb.exec.operator.data;

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Setter
@Getter
@Builder
public class Context {

    private int pin;
    private CommonId indexId;
    private RangeDistribution distribution;
    private List<Boolean> keyState;
    @Builder.Default
    private AtomicBoolean updateResidualDeleteKey = new AtomicBoolean(false);

    private boolean isDuplicateKey;
    // OPTIMISTIC select... for update
    private boolean isShow;

    public Context setPin(int pin) {
        this.pin = pin;
        return this;
    }

    public Context copy() {
        return Context.builder().pin(pin).keyState(keyState).build();
    }

    public void addKeyState(boolean state) {
        keyState.add(state);
    }

    public Boolean[] getKeyState() {
        return keyState.toArray(new Boolean[0]);
    }
}
