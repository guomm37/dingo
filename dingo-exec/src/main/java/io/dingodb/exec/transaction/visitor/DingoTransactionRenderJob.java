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

package io.dingodb.exec.transaction.visitor;

import io.dingodb.common.Location;
import io.dingodb.common.log.LogUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.impl.IdGeneratorImpl;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.visitor.data.CleanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.CleanExtraDataCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.CommitLeaf;
import io.dingodb.exec.transaction.visitor.data.Composite;
import io.dingodb.exec.transaction.visitor.data.Element;
import io.dingodb.exec.transaction.visitor.data.ElementName;
import io.dingodb.exec.transaction.visitor.data.Leaf;
import io.dingodb.exec.transaction.visitor.data.OptimisticRollBackLeaf;
import io.dingodb.exec.transaction.visitor.data.OptimisticRollBackScanLeaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticResidualLockLeaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticRollBackLeaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticRollBackScanLeaf;
import io.dingodb.exec.transaction.visitor.data.PreWriteLeaf;
import io.dingodb.exec.transaction.visitor.data.RollBackLeaf;
import io.dingodb.exec.transaction.visitor.data.RootLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCacheResidualLockLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCleanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCleanExtraDataCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.StreamConverterLeaf;
import io.dingodb.exec.transaction.visitor.data.TransactionElements;
import io.dingodb.exec.transaction.visitor.function.DingoCleanCacheVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoCleanExtraDataCacheVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoCommitVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoOptimisticRollBackScanVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoOptimisticRollBackVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoPessimisticResidualLockVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoPessimisticRollBackScanVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoPessimisticRollBackVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoPreWriteVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoRollBackVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoScanCacheResidualLockVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoScanCacheVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoScanCleanCacheVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoScanCleanExtraDataCacheVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoStreamConverterVisitFun;
import io.dingodb.exec.transaction.visitor.function.DingoTransactionRootVisitFun;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.Collections;

@Slf4j
public class DingoTransactionRenderJob implements Visitor<Collection<Vertex>> {

    private final IdGenerator idGenerator;
    private final Location currentLocation;
    @Getter
    private final Job job;
    private final ITransaction transaction;

    public DingoTransactionRenderJob(Job job,
                                     IdGenerator idGenerator,
                                     Location currentLocation,
                                     ITransaction transaction) {
        this.job = job;
        this.idGenerator = idGenerator;
        this.currentLocation = currentLocation;
        this.transaction = transaction;
    }

    public static void renderPreWriteJob(JobManager jobManager, Job job, Location currentLocation,
                                         ITransaction transaction, boolean checkRoot) {
        try {
            IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
            DingoTransactionRenderJob visitor = new DingoTransactionRenderJob(
                job,
                idGenerator,
                currentLocation,
                transaction
            );
            Element element;
            if (!transaction.getChannelMap().isEmpty()) {
                element = TransactionElements.getElement(ElementName.MULTI_TRANSACTION_PRE_WRITE);
            } else {
                element = TransactionElements.getElement(ElementName.SINGLE_TRANSACTION_PRE_WRITE);
            }
            Collection<Vertex> outputs = element.accept(visitor);
            if (checkRoot && !outputs.isEmpty()) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`.");
            }
            LogUtils.debug(log, "job = {}", job);
        } catch (Exception e) {
            jobManager.removeJob(job.getJobId());
            throw new RuntimeException(e);
        }
    }

    public static void renderCommitJob(JobManager jobManager, Job job, Location currentLocation,
                                       ITransaction transaction, boolean checkRoot) {
        try {
            IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
            DingoTransactionRenderJob visitor = new DingoTransactionRenderJob(
                job,
                idGenerator,
                currentLocation,
                transaction
            );
            Element element;
            if (!transaction.getChannelMap().isEmpty()) {
                element = TransactionElements.getElement(ElementName.MULTI_TRANSACTION_COMMIT);
            } else {
                element = TransactionElements.getElement(ElementName.SINGLE_TRANSACTION_COMMIT);
            }
            Collection<Vertex> outputs = element.accept(visitor);
            if (checkRoot && !outputs.isEmpty()) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`.");
            }
            LogUtils.debug(log, "job = {}", job);
        } catch (Exception e) {
            jobManager.removeJob(job.getJobId());
            throw new RuntimeException(e);
        }
    }

    public static void renderRollBackJob(JobManager jobManager, Job job, Location currentLocation,
                                         ITransaction transaction, boolean checkRoot) {
        try {
            IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
            DingoTransactionRenderJob visitor = new DingoTransactionRenderJob(
                job,
                idGenerator,
                currentLocation,
                transaction
            );
            Element element;
            if (!transaction.getChannelMap().isEmpty()) {
                element = TransactionElements.getElement(ElementName.MULTI_TRANSACTION_ROLLBACK);
            } else {
                element = TransactionElements.getElement(ElementName.SINGLE_TRANSACTION_ROLLBACK);
            }
            Collection<Vertex> outputs = element.accept(visitor);
            if (checkRoot && !outputs.isEmpty()) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`.");
            }
            LogUtils.debug(log, "job = {}", job);
        } catch (Exception e) {
            jobManager.removeJob(job.getJobId());
            throw new RuntimeException(e);
        }
    }

    public static void renderRollBackPessimisticLockJob(JobManager jobManager, Job job, Location currentLocation,
                                                        ITransaction transaction, boolean checkRoot) {
        try {
            IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
            DingoTransactionRenderJob visitor = new DingoTransactionRenderJob(
                job,
                idGenerator,
                currentLocation,
                transaction
            );
            Element element;
            if (!transaction.getChannelMap().isEmpty()) {
                element = TransactionElements.getElement(ElementName.MULTI_TRANSACTION_PESSIMISTIC_ROLLBACK);
            } else {
                element = TransactionElements.getElement(ElementName.SINGLE_TRANSACTION_PESSIMISTIC_ROLLBACK);
            }
            Collection<Vertex> outputs = element.accept(visitor);
            if (checkRoot && !outputs.isEmpty()) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`.");
            }
            LogUtils.debug(log, "job = {}", job);
        } catch (Exception e) {
            jobManager.removeJob(job.getJobId());
            throw new RuntimeException(e);
        }
    }

    public static void renderRollBackOptimisticData(JobManager jobManager, Job job, Location currentLocation,
                                                        ITransaction transaction, boolean checkRoot) {
        try {
            IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
            DingoTransactionRenderJob visitor = new DingoTransactionRenderJob(
                job,
                idGenerator,
                currentLocation,
                transaction
            );
            Element element;
            if (!transaction.getChannelMap().isEmpty()) {
                element = TransactionElements.getElement(ElementName.MULTI_TRANSACTION_OPTIMISTIC_ROLLBACK);
            } else {
                element = TransactionElements.getElement(ElementName.SINGLE_TRANSACTION_OPTIMISTIC_ROLLBACK);
            }
            Collection<Vertex> outputs = element.accept(visitor);
            if (checkRoot && !outputs.isEmpty()) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`.");
            }
            LogUtils.debug(log, "job = {}", job);
        } catch (Exception e) {
            jobManager.removeJob(job.getJobId());
            throw new RuntimeException(e);
        }
    }

    public static void renderRollBackResidualPessimisticLockJob(JobManager jobManager, Job job, Location currentLocation,
                                                                ITransaction transaction, boolean checkRoot) {
        try {
            IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
            DingoTransactionRenderJob visitor = new DingoTransactionRenderJob(
                job,
                idGenerator,
                currentLocation,
                transaction
            );
            Element element;
            if (!transaction.getChannelMap().isEmpty()) {
                element = TransactionElements.getElement(ElementName.MULTI_TRANSACTION_RESIDUAL_LOCK);
            } else {
                element = TransactionElements.getElement(ElementName.SINGLE_TRANSACTION_RESIDUAL_LOCK);
            }
            Collection<Vertex> outputs = element.accept(visitor);
            if (checkRoot && !outputs.isEmpty()) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`.");
            }
            LogUtils.debug(log, "job = {}", job);
        } catch (Exception e) {
            jobManager.removeJob(job.getJobId());
            throw new RuntimeException(e);
        }
    }

    public static void renderCleanCacheJob(JobManager jobManager, Job job, Location currentLocation,
                                                        ITransaction transaction, boolean checkRoot) {
        try {
            IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
            DingoTransactionRenderJob visitor = new DingoTransactionRenderJob(
                job,
                idGenerator,
                currentLocation,
                transaction
            );
            Element element;
            if (!transaction.getChannelMap().isEmpty()) {
                element = TransactionElements.getElement(ElementName.MULTI_TRANSACTION_CLEAN_CACHE);
            } else {
                element = TransactionElements.getElement(ElementName.SINGLE_TRANSACTION_CLEAN_CACHE);
            }
            Collection<Vertex> outputs = element.accept(visitor);
            if (checkRoot && !outputs.isEmpty()) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`.");
            }
            LogUtils.debug(log, "job = {}", job);
        } catch (Exception e) {
            jobManager.removeJob(job.getJobId());
            throw new RuntimeException(e);
        }
    }

    public static void renderCleanExtraDataCacheJob(JobManager jobManager, Job job, Location currentLocation,
                                           ITransaction transaction, boolean checkRoot) {
        try {
            IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
            DingoTransactionRenderJob visitor = new DingoTransactionRenderJob(
                job,
                idGenerator,
                currentLocation,
                transaction
            );
            Element element;
            if (!transaction.getChannelMap().isEmpty()) {
                element = TransactionElements.getElement(ElementName.MULTI_TRANSACTION_CLEAN_EXTRA_CACHE);
            } else {
                element = TransactionElements.getElement(ElementName.SINGLE_TRANSACTION_CLEAN_EXTRA_CACHE);
            }
            Collection<Vertex> outputs = element.accept(visitor);
            if (checkRoot && !outputs.isEmpty()) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`.");
            }
            LogUtils.debug(log, "job = {}", job);
        } catch (Exception e) {
            jobManager.removeJob(job.getJobId());
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Vertex> visit(Leaf leaf) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Vertex> visit(RootLeaf rootLeaf) {
        return DingoTransactionRootVisitFun.visit(job, idGenerator, currentLocation, transaction, this, rootLeaf);
    }

    @Override
    public Collection<Vertex> visit(@NonNull ScanCacheLeaf scanCacheLeaf) {
        return DingoScanCacheVisitFun.visit(job, idGenerator, currentLocation, transaction, this, scanCacheLeaf);
    }

    @Override
    public Collection<Vertex> visit(PreWriteLeaf preWriteLeaf) {
        return DingoPreWriteVisitFun.visit(job, idGenerator, currentLocation, transaction, this, preWriteLeaf);
    }

    @Override
    public Collection<Vertex> visit(StreamConverterLeaf streamConverterLeaf) {
        return DingoStreamConverterVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, streamConverterLeaf);
    }

    @Override
    public Collection<Vertex> visit(CommitLeaf commitLeaf) {
        return DingoCommitVisitFun.visit(job, idGenerator, currentLocation, transaction, this, commitLeaf);
    }

    @Override
    public Collection<Vertex> visit(RollBackLeaf rollBackLeaf) {
        return DingoRollBackVisitFun.visit(job, idGenerator, currentLocation, transaction, this, rollBackLeaf);
    }

    @Override
    public Collection<Vertex> visit(PessimisticRollBackLeaf pessimisticRollBackLeaf) {
        return DingoPessimisticRollBackVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, pessimisticRollBackLeaf);
    }

    @Override
    public Collection<Vertex> visit(PessimisticRollBackScanLeaf pessimisticRollBackScanLeaf) {
        return DingoPessimisticRollBackScanVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, pessimisticRollBackScanLeaf);
    }

    @Override
    public Collection<Vertex> visit(OptimisticRollBackLeaf optimisticRollBackLeaf) {
        return DingoOptimisticRollBackVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, optimisticRollBackLeaf);
    }

    @Override
    public Collection<Vertex> visit(OptimisticRollBackScanLeaf optimisticRollBackScanLeaf) {
        return DingoOptimisticRollBackScanVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, optimisticRollBackScanLeaf);
    }

    @Override
    public Collection<Vertex> visit(ScanCleanCacheLeaf scanCleanCacheLeaf) {
        return DingoScanCleanCacheVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, scanCleanCacheLeaf);
    }

    @Override
    public Collection<Vertex> visit(PessimisticResidualLockLeaf pessimisticResidualLockLeaf) {
        return DingoPessimisticResidualLockVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, pessimisticResidualLockLeaf);
    }

    @Override
    public Collection<Vertex> visit(ScanCacheResidualLockLeaf scanCacheResidualLockLeaf) {
        return DingoScanCacheResidualLockVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, scanCacheResidualLockLeaf);
    }

    @Override
    public Collection<Vertex> visit(CleanExtraDataCacheLeaf cleanExtraDataCacheLeaf) {
        return DingoCleanExtraDataCacheVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, cleanExtraDataCacheLeaf);
    }

    @Override
    public Collection<Vertex> visit(ScanCleanExtraDataCacheLeaf scanCleanExtraDataCacheLeaf) {
        return DingoScanCleanExtraDataCacheVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, scanCleanExtraDataCacheLeaf);
    }

    @Override
    public Collection<Vertex> visit(CleanCacheLeaf cleanCacheLeaf) {
        return DingoCleanCacheVisitFun.visit(job, idGenerator, currentLocation,
            transaction, this, cleanCacheLeaf);
    }

    @Override
    public Collection<Vertex> visit(Composite composite) {
        return null;
    }

}
