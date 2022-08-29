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

package io.dingodb.mpu.storage.rocks;

import io.dingodb.mpu.storage.KV;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static io.dingodb.common.util.ByteArrayUtils.lessThan;
import static io.dingodb.common.util.ByteArrayUtils.lessThanOrEqual;

@Slf4j
@SuppressWarnings("checkstyle:NoFinalizer")
public class Reader implements io.dingodb.mpu.storage.Reader {
    private final RocksDB db;
    private final ColumnFamilyHandle handle;
    private final Snapshot snapshot;
    private final ReadOptions readOptions;

    public Reader(RocksDB db, ColumnFamilyHandle handle) {
        this.db = db;
        this.snapshot = db.getSnapshot();
        this.handle = handle;
        this.readOptions = new ReadOptions().setSnapshot(snapshot);
    }

    public Iterator iterator() {
            return new Iterator(db.newIterator(handle, readOptions), null, null, true, true);
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return db.get(handle, readOptions, key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public List<KV> get(List<byte[]> keys) {
        try {
            List<byte[]> values = db.multiGetAsList(readOptions, Collections.singletonList(handle), keys);
            List<KV> entries = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                entries.add(new KV(keys.get(i), values.get(i)));
            }
            return entries;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean containsKey(byte[] key) {
        try {
            return db.get(handle, readOptions, key) != null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterator scan(byte[] startKey, byte[] endKey, boolean withStart, boolean withEnd) {
        return new Iterator(db.newIterator(handle, readOptions), startKey, endKey, withStart, withEnd);
    }

    @Override
    public long count() {
        return count(null, null, true, true);
    }

    public long count(byte[] start, byte[] end, boolean withStart, boolean withEnd) {
        long count = 0;
        start = start;
        end = end;
        try (RocksIterator iterator = db.newIterator(handle, readOptions)) {
            if (start == null) {
                iterator.seekToFirst();
            } else {
                iterator.seek(start);
                if (iterator.isValid() && !withStart && Arrays.equals(iterator.key(), start)) {
                    iterator.next();
                }
            }
            while (iterator.isValid() && (end == null || (withEnd
                                                          ? lessThanOrEqual(end, iterator.key())
                                                          : lessThan(end, iterator.key())))) {
                count++;
                iterator.next();
            }
        }
        return count;
    }

    // close read options, snapshot
    @Override
    protected void finalize() throws Throwable {
        readOptions.close();
        snapshot.close();
    }

    static class Iterator implements java.util.Iterator<KV> {

        private final RocksIterator iterator;
        private final Predicate<byte[]> _end;
        private KV current;

        Iterator(RocksIterator iterator, byte[] start, byte[] end, boolean withStart, boolean withEnd) {
            this.iterator = iterator;
            this._end = end == null ? __ -> true : withEnd ? __ -> lessThanOrEqual(__, end) : __ -> lessThan(__, end);
            if (start == null) {
                this.iterator.seekToFirst();
            } else {
                this.iterator.seek(start);
            }
            if (this.iterator.isValid() && !withStart && Arrays.equals(this.iterator.key(), start)) {
                this.iterator.next();
            }
            if (this.iterator.isValid()) {
                this.current = new KV(this.iterator.key(), this.iterator.value());
            }
            this.iterator.next();
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public KV next() {
            KV kv = current;
            if (kv == null) {
                throw new NoSuchElementException();
            }
            iterator.next();
            if (iterator.isValid() && _end.test(iterator.key())) {
                this.current = new KV(iterator.key(), iterator.value());
            } else {
                this.current = null;
            }
            return kv;
        }

        // close rocksdb iterator
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            try {
                iterator.close();
            } catch (Exception e) {
                log.error("Close iterator on finalize error.", e);
            }
        }
    }
}
