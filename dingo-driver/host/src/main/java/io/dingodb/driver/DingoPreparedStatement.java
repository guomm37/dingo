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

package io.dingodb.driver;

import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.profile.SqlProfile;
import io.dingodb.driver.type.converter.TypedValueConverter;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.exception.TaskCancelException;
import io.dingodb.exec.transaction.base.TxnPartData;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.ByteString;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DingoPreparedStatement extends AvaticaPreparedStatement {

    // for mysql protocol prepare multi addBatch
    @Getter
    @Setter
    private Integer[] types;

    @Setter
    @Getter
    private boolean hasIncId;

    @Setter
    @Getter
    private Long autoIncId;

    @Getter
    @Setter
    private SqlProfile sqlProfile;

    protected DingoPreparedStatement(
        DingoConnection connection,
        Meta.StatementHandle handle,
        Meta.Signature signature,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        super(
            connection,
            handle,
            signature,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
        sqlProfile = new SqlProfile("executePrepare", true);
        sqlProfile.setSql(getSql());
        sqlProfile.setStatementType(signature.statementType.toString().toLowerCase());
        sqlProfile.setSchema(connection.getContext().getUsedSchema().getName());
        String user = connection.getContext().getOption("user");
        String host = connection.getContext().getOption("host");
        sqlProfile.setSimpleUser(user + "@" + host);
        sqlProfile.setInstance(DingoConfiguration.location().toString());
        if (signature instanceof DingoSignature) {
            DingoSignature dingoSignature = (DingoSignature) signature;
            if (dingoSignature.getFullyTableList() != null) {
                sqlProfile.setFullyTableList(dingoSignature.getFullyTableList());
            }
        }
    }

    void setParameterValues(@NonNull List<TypedValue> parameterValues) {
        for (int i = 0; i < parameterValues.size(); ++i) {
            slots[i] = parameterValues.get(i);
        }
    }

    @Override
    protected void setSignature(Meta.Signature signature) {
        super.setSignature(signature);
    }

    void createResultSet(Meta.@Nullable Frame firstFrame) throws SQLException {
        if (openResultSet != null) {
            openResultSet.close();
        }
        Meta.Signature signature = getSignature();
        openResultSet = ((DingoConnection) connection).newResultSet(
            this,
            signature,
            firstFrame,
            signature.sql
        );
    }

    @NonNull
    public Iterator<Object[]> createIterator(@NonNull JobManager jobManager) {
        Meta.Signature signature = getSignature();
        if (signature instanceof DingoSignature) {
            try {
                Object[] parasValue = TypedValue.values(getParameterValues()).toArray();
                for (int i = 0; i < parasValue.length; i ++) {
                    if (parasValue[i] instanceof ByteString) {
                        parasValue[i] = ((ByteString) parasValue[i]).getBytes();
                    }
                }
                CommonId jobId = ((DingoSignature) signature).getJobId();
                if (jobId == null) {
                    List<Object[]> empty = new ArrayList<>();
                    return empty.iterator();
                }
                Job job = jobManager.getJob(jobId);
                Object[] paras = ((Object[]) job.getParasType().convertFrom(
                    parasValue,
                    new TypedValueConverter(getCalendar())
                ));
                return jobManager.createIterator(job, paras);
            } catch (NullPointerException e) {
                throw new IllegalStateException("Not all parameters are set.");
            }
        }
        throw ExceptionUtils.wrongSignatureType(this, signature);
    }

    @SneakyThrows
    public Map<TxnPartData, Boolean> getJobPartData(@NonNull JobManager jobManager) {
        Meta.Signature signature = getSignature();
        if (signature instanceof DingoSignature) {
            if (cancelFlag.get()) {
                throw new TaskCancelException("task is cancel");
            }
            Job job = jobManager.getJob(((DingoSignature) signature).getJobId());
            Map<TxnPartData, Boolean> partData = jobManager.getPartData(job);
            if (cancelFlag.get()) {
                throw new TaskCancelException("task is cancel");
            }
            return partData;
        }
        return null;
    }

    public Job getJob(@NonNull JobManager jobManager) {
        Meta.Signature signature = getSignature();
        if (signature instanceof DingoSignature) {
            CommonId jobId = ((DingoSignature) signature).getJobId();
            return jobManager.getJob(jobId);
        }
        return null;
    }

    public void setTxnId(@NonNull JobManager jobManager, @NonNull CommonId txnId) {
        Meta.Signature signature = getSignature();
        if (signature instanceof DingoSignature) {
            CommonId jobId = ((DingoSignature) signature).getJobId();
            Job job = jobManager.getJob(jobId);
            job.setTxnId(txnId);
        }
    }

    public CommonId getJobId(@NonNull JobManager jobManager) {
        Meta.Signature signature = getSignature();
        if (signature instanceof DingoSignature) {
            CommonId jobId = ((DingoSignature) signature).getJobId();
            Job job = jobManager.getJob(jobId);
            return job.getJobId();
        }
        return null;
    }

    public String getSql() {
        Meta.Signature signature = getSignature();
        return signature.sql;
    }

    public boolean isDml() {
        Meta.Signature sh = getSignature();
        return (sh.statementType == Meta.StatementType.DELETE
            || sh.statementType == Meta.StatementType.INSERT
            || sh.statementType == Meta.StatementType.UPDATE
            || sh.statementType == Meta.StatementType.IS_DML);
    }

    public void removeJob(JobManager jobManager) {
        Meta.Signature signature = getSignature();
        DingoStatementUtils.removeJobInSignature(jobManager, signature);
    }

    /**
     * long bytes need append to slots on prepare statement.
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param bytesVal the parameter value
     * @throws SQLException e
     */
    @Override
    public void setBytes(int parameterIndex, byte[] bytesVal) throws SQLException {
        if (slots.length >= parameterIndex) {
            TypedValue value = slots[parameterIndex - 1];
            if (value == null) {
                super.setBytes(parameterIndex, bytesVal);
            } else {
                // base64 encode
                String valStr = (String) value.value;
                byte[] preBytes = ByteString.ofBase64(valStr).getBytes();
                byte[] bytes = new byte[preBytes.length + bytesVal.length];
                System.arraycopy(preBytes, 0, bytes, 0, preBytes.length);
                System.arraycopy(bytesVal, 0, bytes, preBytes.length, bytesVal.length);
                super.setBytes(parameterIndex, bytes);
            }
        } else {
            super.setBytes(parameterIndex, bytesVal);
        }
    }

    /**
     * prepare statement add batch : first with type to cache,secondly without type.
     * @param types data schema
     */
    public void setBoundTypes(Integer[] types) {
        if (this.types == null && types != null) {
            this.types = types;
        }
    }
}
