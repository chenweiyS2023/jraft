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

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.example.counter.rpc.CounterOutter.IncrementAndGetRequest;
import com.alipay.sofa.jraft.example.counter.rpc.CounterOutter.SetBytesValueRequest;
import com.alipay.sofa.jraft.example.counter.rpc.CounterOutter.GetBytesValueRequest;
import com.alipay.sofa.jraft.example.counter.rpc.CounterGrpcHelper;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class CounterClient {

    public static void main(final String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage : java com.alipay.sofa.jraft.example.counter.CounterClient {groupId} {conf}");
            System.out
                .println("Example: java com.alipay.sofa.jraft.example.counter.CounterClient counter 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        final String groupId = args[0];
        final String confStr = args[1];
        CounterGrpcHelper.initGRpc();

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
        final int n = 1000;
        final CountDownLatch latch = new CountDownLatch(n);
        final long start = System.currentTimeMillis();
        // for (int i = 0; i < n; i++) {
        // incrementAndGet(cliClientService, leader, i, latch);
        // }
        setBytesValue(cliClientService, leader, "hello".getBytes(), latch);
        // latch.await();
        System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " mssssssss.");

        // sleep for 5s to wait for the data to be replicated to the follower
        Thread.sleep(5000);

        // get the ip addresses of the cluster
        getBytesValue(cliClientService);

        // Thread.sleep(15000);
        // latch.await();
        System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " mssssssss.");
        System.exit(0);
    }

    private static void incrementAndGet(final CliClientServiceImpl cliClientService, final PeerId leader,
                                        final long delta, CountDownLatch latch) throws RemotingException,
                                                                               InterruptedException {
        IncrementAndGetRequest request = IncrementAndGetRequest.newBuilder().setDelta(delta).build();
        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("incrementAndGet result:" + result);
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

    private static void setBytesValue(final CliClientServiceImpl cliClientService, final PeerId leader,
                                      final byte[] bytes, CountDownLatch latch) throws RemotingException,
                                                                               InterruptedException {
        SetBytesValueRequest request = SetBytesValueRequest.newBuilder()
            .setValue(com.google.protobuf.ByteString.copyFrom(bytes)).build();
        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("setBytesValue result:" + result);
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

    private static void getBytesValue(final CliClientServiceImpl cliClientService) throws RemotingException,
                                                                                  InterruptedException {
        GetBytesValueRequest request = GetBytesValueRequest.newBuilder().build();

        // send the request to all the peers in the cluster

        // get the ip addresses of the cluster
        final Configuration conf = RouteTable.getInstance().getConfiguration("counter");
        for (PeerId peer : conf) {
            System.out.println("peer:" + peer.getEndpoint());
            // cliClientService.getRpcClient().invokeAsync(peer.getEndpoint(), request, new
            // InvokeCallback() {

            // @Override
            // public void complete(Object result, Throwable err) {
            // if (err == null) {
            // System.out.println("getBytesValue result:" + result);
            // } else {
            // err.printStackTrace();
            // }
            // }

            // @Override
            // public Executor executor() {
            // return null;
            // }
            // }, 15000);

            // invokeSync and print the result
            Object result = cliClientService.getRpcClient().invokeSync(peer.getEndpoint(), request, 15000);

            System.out.println("getBytesValue result:" + result.toString());

            // cliClientService.getRpcClient().invokeSync(peer.getEndpoint(), request,
            // 15000);
        }
        // cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request,
        // new InvokeCallback() {

        // @Override
        // public void complete(Object result, Throwable err) {
        // if (err == null) {
        // latch.countDown();
        // System.out.println("getBytesValue result:" + result);
        // } else {
        // err.printStackTrace();
        // latch.countDown();
        // }
        // }

        // @Override
        // public Executor executor() {
        // return null;
        // }
        // }, 5000);
    }

}
