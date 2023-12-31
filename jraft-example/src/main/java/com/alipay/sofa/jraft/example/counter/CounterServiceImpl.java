/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.example.counter;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StoreEngineHelper;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
// import ArrayUtil
import java.util.Arrays;

/**
 * @author likun (saimu.msm@antfin.com)
 */
public class CounterServiceImpl implements CounterService {
    private static final Logger LOG = LoggerFactory.getLogger(CounterServiceImpl.class);

    private final CounterServer counterServer;
    private final Executor      readIndexExecutor;

    public CounterServiceImpl(CounterServer counterServer) {
        this.counterServer = counterServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    private Executor createReadIndexExecutor() {
        final StoreEngineOptions opts = new StoreEngineOptions();
        return StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
    }

    @Override
    public void get(final boolean readOnlySafe, final CounterClosure closure) {
        if (!readOnlySafe) {
            closure.success(getValue());
            closure.run(Status.OK());
            return;
        }

        this.counterServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    closure.success(getValue());
                    closure.run(Status.OK());
                    return;
                }
                CounterServiceImpl.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.debug("Fail to get value with 'ReadIndex': {}, try to applying to the state machine.",
                                status);
                        applyOperation(CounterOperation.createGet(), closure);
                    } else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

    private boolean isLeader() {
        return this.counterServer.getFsm().isLeader();
    }

    private long getValue() {
        return this.counterServer.getFsm().getValue();
    }

    private String getRedirect() {
        return this.counterServer.redirect().getRedirect();
    }

    @Override
    public void incrementAndGet(final long delta, final CounterClosure closure) {
        applyOperation(CounterOperation.createIncrement(delta), closure);
    }

    @Override
    public void setBytesValue(final byte[] value, final CounterClosure closure) {
        applyOperation(CounterOperation.createSetBytesValue(value), closure);
    }

    @Override
    public void getBytesValue(final CounterClosure closure) {
        System.out.println("getBytesValue entered");
        Byte[] byteArray = this.counterServer.getFsm().getBytesValue();
        // print byteArray content
        System.out.println("byteArray value");
        for (int i = 0; i < byteArray.length; i++) {
            System.out.println(byteArray[i]);
        }
        byte[] byteObjects = new byte[byteArray.length];
        int j = 0;
        for (Byte b : byteArray) {
            // skip type cast if byte is null
            if (b != null)
                byteObjects[j] = b.byteValue();
            j++;
        }

        System.out.println("byteObjects assigned value");
        for (int i = 0; i < byteObjects.length; i++) {
            System.out.println(byteObjects[i]);
        }
        LOG.info("Get byte value={} length={} at logIndex={}", byteArray, byteArray.length);
        closure.successWithBytes(byteObjects);
        closure.run(Status.OK());
        return;
    }

    private void applyOperation(final CounterOperation op, final CounterClosure closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }

        try {
            closure.setCounterOperation(op);
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(op)));
            task.setDone(closure);
            this.counterServer.getNode().apply(task);
        } catch (CodecException e) {
            String errorMsg = "Fail to encode CounterOperation";
            LOG.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    private void handlerNotLeaderError(final CounterClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }
}
