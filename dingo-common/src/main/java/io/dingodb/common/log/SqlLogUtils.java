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

package io.dingodb.common.log;

import io.dingodb.common.util.StackTraces;
import lombok.extern.slf4j.Slf4j;

import static io.dingodb.common.util.StackTraces.CURRENT_STACK;

@Slf4j
public final class SqlLogUtils {
    private SqlLogUtils() {}
    public static void info(String message, Object... args) {
        if (log.isInfoEnabled()) {
            log.info(
                StackTraces.stack(
                    CURRENT_STACK + 1,
                    StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message),
                args
            );
        }
    }

    public static void debug(String message, Object... args) {
        if (log.isDebugEnabled()) {
            log.debug(
                StackTraces.stack(
                    CURRENT_STACK + 1,
                    StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message),
                args
            );
        }
    }

    public static void trace(String message, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(
                StackTraces.stack(
                    CURRENT_STACK + 1,
                    StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message),
                args
            );
        }
    }

    public static void warn(String message, Object... args) {
        if (log.isWarnEnabled()) {
            log.warn(
                StackTraces.stack(
                    CURRENT_STACK + 1,
                    StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message),
                args
            );
        }
    }

    public static void error(String message, Object... args) {
        if (log.isErrorEnabled()) {
            log.error(
                StackTraces.stack(
                    CURRENT_STACK + 1,
                    StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message),
                args
            );
        }
    }

    public static void error(String message, Throwable error) {
        if (log.isErrorEnabled()) {
            log.error(
                StackTraces.stack(
                    CURRENT_STACK + 1,
                    StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message),
                error
            );
        }
    }
}
