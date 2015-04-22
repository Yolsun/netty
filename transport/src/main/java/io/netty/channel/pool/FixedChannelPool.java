/*
 * Copyright 2015 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.OneTimeTask;

import java.util.ArrayDeque;
import java.util.Queue;
/**
 * {@link ChannelPool} implementation that takes another {@link ChannelPool} implementation and enfore a maximum
 * number of concurrent connections.
 *
 * @param <C>   the {@link Channel} type to pool.
 * @param <K>   the {@link ChannelPoolKey} that is used to store and lookup the {@link Channel}s.
 */
public final class FixedChannelPool<C extends Channel, K extends ChannelPoolKey> extends SimpleChannelPool<C, K> {
    private static final IllegalStateException FULL_EXCEPTION =
            new IllegalStateException("Too many outstanding acquire operations");
    static {
        FULL_EXCEPTION.setStackTrace(EmptyArrays.EMPTY_STACK_TRACE);
    }

    private final EventExecutor executor;

    // There is no need to worry about synchronzation as everything that modified the queue or counts is done
    // by the above EventExecutor.
    private final Queue<AcquireTask<C, K>> pendingAcquireQueue = new ArrayDeque<AcquireTask<C, K>>();
    private final int maxConnections;
    private final int maxPendingAcquires;
    private int acquiredChannelCount;
    private int pendingAcquireCount;

    /**
     * Creates a new instance using the {@link ActiveChannelHealthChecker} and a {@link ChannelPoolSegmentFactory} that
     * process things in LIFO order.
     *
     * @param bootstrap         the {@link Bootstrap} that is used for connections
     * @param handler           the {@link ChannelPoolHandler} that will be notified for the different pool actions
     * @param maxConnections    the numnber of maximal active connections, once this is reached new tries to acquire
     *                          a {@link Channel} will be delayed until a connection is returned to the pool again.
     */
    public FixedChannelPool(Bootstrap bootstrap,
                            ChannelPoolHandler<C, K> handler, int maxConnections) {
        this(bootstrap, handler, maxConnections, Integer.MAX_VALUE);
    }
    /**
     * Creates a new instance using the {@link ActiveChannelHealthChecker} and a {@link ChannelPoolSegmentFactory} that
     * process things in LIFO order.
     *
     * @param bootstrap             the {@link Bootstrap} that is used for connections
     * @param handler               the {@link ChannelPoolHandler} that will be notified for the different pool actions
     * @param maxConnections        the numnber of maximal active connections, once this is reached new tries to
     *                              acquire a {@link Channel} will be delayed until a connection is returned to the
     *                              pool again.
     * @param maxPendingAcquires    the maximum number of pending acquires. Once this is exceed acquire tries will
     *                              be failed.
     */
    public FixedChannelPool(Bootstrap bootstrap,
                            ChannelPoolHandler<C, K> handler, int maxConnections, int maxPendingAcquires) {
        super(bootstrap, handler);
        executor = bootstrap.group().next();
        this.maxConnections = maxConnections;
        this.maxPendingAcquires = maxPendingAcquires;
    }

    /**
     * Creates a new instance.
     *
     * @param bootstrap             the {@link Bootstrap} that is used for connections
     * @param handler               the {@link ChannelPoolHandler} that will be notified for the different pool actions
     * @param healthCheck           the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
     *                              still healty when obtain from the {@link ChannelPool}
     * @param segmentFactory        the {@link ChannelPoolSegmentFactory} that will be used to create new
     *                              {@link ChannelPoolSegmentFactory.ChannelPoolSegment}s when needed
     * @param maxConnections        the numnber of maximal active connections, once this is reached new tries to
     *                              acquire a {@link Channel} will be delayed until a connection is returned to the
     *                              pool again.
     * @param maxPendingAcquires    the maximum number of pending acquires. Once this is exceed acquire tries will
     *                              be failed.
     */
    public FixedChannelPool(Bootstrap bootstrap,
                            ChannelPoolHandler<C, K> handler,
                            ChannelHealthChecker<C, K> healthCheck,
                            ChannelPoolSegmentFactory<C> segmentFactory,
                            int maxConnections, int maxPendingAcquires) {
        super(bootstrap, handler, healthCheck, segmentFactory);
        if (maxConnections < 1) {
            throw new IllegalArgumentException("maxConnections: " + maxConnections + " (expected: >= 1)");
        }
        if (maxPendingAcquires < 1) {
            throw new IllegalArgumentException("maxPendingAcquires: " + maxPendingAcquires + " (expected: >= 1)");
        }
        executor = bootstrap.group().next();
        this.maxConnections = maxConnections;
        this.maxPendingAcquires = maxPendingAcquires;
    }

    @Override
    public Future<C> acquire(final K key, final Promise<C> promise) {
        try {
            final Promise<C> p = executor.newPromise();
            if (executor.inEventLoop()) {
                acquire0(key, promise, p);
            } else {
                executor.execute(new OneTimeTask() {
                    @Override
                    public void run() {
                        acquire0(key, promise, p);
                    }
                });
            }
            return p;
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }
        return promise;
    }

    private void acquire0(K key, final Promise<C> originalPromise, Promise<C> p) {
        assert executor.inEventLoop();

        p.addListener(new FutureListener<C>() {
            @Override
            public void operationComplete(Future<C> future) throws Exception {
                assert executor.inEventLoop();

                if (future.isSuccess()) {
                    originalPromise.setSuccess(future.getNow());
                } else {
                    // Something went wrong try to run pending acquire tasks.
                    --acquiredChannelCount;

                    assert acquiredChannelCount > 0;

                    // Run the pending acquire tasks before notify the original promise so if the user would
                    // try to acquire again from the ChannelFutureListener and the pendingAcquireCount is >=
                    // maxPendingAcquires we may be able to run some pending tasks first and so allow to add
                    // more.
                    runTaskQueue();
                    originalPromise.setFailure(future.cause());
                }
            }
        });
        if (acquiredChannelCount < maxConnections) {
            ++acquiredChannelCount;

            assert acquiredChannelCount > 0;

            super.acquire(key, p);
        } else {
            if (++pendingAcquireCount > maxPendingAcquires) {
                originalPromise.setFailure(FULL_EXCEPTION);
            } else {
                pendingAcquireQueue.offer(new AcquireTask<C, K>(key, p));
            }

            assert pendingAcquireCount > 0;
        }
    }

    @Override
    public Future<Boolean> release(final C channel, final Promise<Boolean> promise) {
        try {
            final Promise<Boolean> p = executor.newPromise();
            super.release(channel, p.addListener(new FutureListener<Boolean>() {

                @Override
                public void operationComplete(Future<Boolean> future) throws Exception {
                    assert executor.inEventLoop();

                    if (future.isSuccess()) {
                        Boolean result = future.getNow();
                        if (result == Boolean.TRUE) {
                            --acquiredChannelCount;

                            assert acquiredChannelCount >= 0;

                            // Run the pending acquire tasks before notify the original promise so if the user would
                            // try to acquire again from the ChannelFutureListener and the pendingAcquireCount is >=
                            // maxPendingAcquires we may be able to run some pending tasks first and so allow to add
                            // more.
                            runTaskQueue();
                        }
                        promise.setSuccess(result);
                    } else {
                        promise.setFailure(future.cause());
                    }
                }
            }));
            return p;
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }
        return promise;
    }

    private void runTaskQueue() {
        while (acquiredChannelCount <= maxConnections) {
            AcquireTask<C, K> task = pendingAcquireQueue.poll();
            if (task == null) {
                break;
            }
            --pendingAcquireCount;
            ++acquiredChannelCount;

            super.acquire(task.key, task.promise);
        }

        assert pendingAcquireCount >= 0;
        assert acquiredChannelCount >= 0;
    }

    private static final class AcquireTask<C, K> {
        private final K key;
        private final Promise<C> promise;

        public AcquireTask(final K key, Promise<C> promise) {
            this.key = key;
            this.promise = promise;
        }
    }
}
