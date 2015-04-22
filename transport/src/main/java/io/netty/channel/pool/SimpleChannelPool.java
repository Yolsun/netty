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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelPoolSegmentFactory.ChannelPoolSegment;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.OneTimeTask;
import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.ConcurrentMap;

import static io.netty.util.internal.ObjectUtil.checkNotNull;

/**
 * Simple {@link ChannelPool} implementation which will create new {@link Channel}s if someone tries to acquire
 * a {@link Channel} but none is in the pool atm. No limit on the maximal concurrent {@link Channel}s is enforced.
 *
 * @param <C>   the {@link Channel} type to pool.
 * @param <K>   the {@link ChannelPoolKey} that is used to store and lookup the {@link Channel}s.
 */
public class SimpleChannelPool<C extends Channel, K extends ChannelPoolKey> implements ChannelPool<C, K> {

    private static final AttributeKey<ChannelPoolKey> KEY = AttributeKey.newInstance("channelPoolKey");
    private final ConcurrentMap<ChannelPoolKey, ChannelPoolSegment<C>> pool = PlatformDependent.newConcurrentHashMap();
    private final ChannelPoolHandler<C, K> handler;
    private final ChannelHealthChecker<C, K> healthCheck;
    private final ChannelPoolSegmentFactory<C> segmentFactory;
    private final Bootstrap bootstrap;

    /**
     * Creates a new instance using the {@link ActiveChannelHealthChecker} and a {@link ChannelPoolSegmentFactory} that
     * process things in LIFO order.
     *
     * @param bootstrap         the {@link Bootstrap} that is used for connections
     * @param handler           the {@link ChannelPoolHandler} that will be notified for the different pool actions
     */
    public SimpleChannelPool(Bootstrap bootstrap, final ChannelPoolHandler<C, K> handler) {
        this(bootstrap, handler,
             ActiveChannelHealthChecker.<C, K>instance(), ChannelPoolSegmentFactories.<C>newLifoFactory());
    }

    /**
     * Creates a new instance.
     *
     * @param bootstrap         the {@link Bootstrap} that is used for connections
     * @param handler           the {@link ChannelPoolHandler} that will be notified for the different pool actions
     * @param healthCheck       the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
     *                          still healty when obtain from the {@link ChannelPool}
     * @param segmentFactory    the {@link ChannelPoolSegmentFactory} that will be used to create new
     *                          {@link ChannelPoolSegment}s when needed
     */
    public SimpleChannelPool(Bootstrap bootstrap, final ChannelPoolHandler<C, K> handler,
                             final ChannelHealthChecker<C, K> healthCheck,
                             final ChannelPoolSegmentFactory<C> segmentFactory) {
        this.handler = checkNotNull(handler, "handler");
        this.healthCheck = checkNotNull(healthCheck, "healthCheck");
        this.segmentFactory = checkNotNull(segmentFactory, "segmentFactory");
        this.bootstrap = checkNotNull(bootstrap, "bootstrap").clone();
        this.bootstrap.handler(new ChannelInitializer<C>() {
            @SuppressWarnings("unchecked")
            @Override
            protected void initChannel(C ch) throws Exception {
                assert ch.eventLoop().inEventLoop();
                K key = (K) ch.attr(KEY).get();
                assert key != null;
                handler.channelCreated(ch, key);
            }
        });
    }

    private EventLoop loop(K key) {
        EventLoop loop = key.eventLoop();
        if (loop == null) {
            loop = bootstrap.group().next();
        }
        return loop;
    }

    @Override
    public final Future<C> acquire(K key) {
        return acquire(key, loop(key).<C>newPromise());
    }

    @Override
    public Future<C> acquire(final K key, final Promise<C> promise) {
        checkNotNull(key, "key");
        checkNotNull(promise, "promise");
        try {
            ChannelPoolSegment<C> channels = pool.get(key);
            if (channels == null) {
                newChannel(key, promise);
                return promise;
            }

            final C ch = channels.poll();
            if (ch == null) {
                newChannel(key, promise);
                return promise;
            }
            EventLoop loop = ch.eventLoop();
            if (loop.inEventLoop()) {
                doHealthCheck(key, ch, promise);
            } else {
                loop.execute(new OneTimeTask() {
                    @Override
                    public void run() {
                        doHealthCheck(key, ch, promise);
                    }
                });
            }
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }
        return promise;
    }

    private void doHealthCheck(final K key, final C ch, final Promise<C> promise) {
        assert ch.eventLoop().inEventLoop();

        Future<Boolean> f = healthCheck.isHealthy(ch, key);
        if (f.isDone()) {
            notifyHealthCheck(f, ch, key, promise);
        } else {
            f.addListener(new FutureListener<Boolean>() {
                @Override
                public void operationComplete(Future<Boolean> future) throws Exception {
                    notifyHealthCheck(future, ch, key, promise);
                }
            });
        }
    }

    private void notifyHealthCheck(Future<? super Boolean> future, C ch, K key, Promise<C> promise) {
        assert ch.eventLoop().inEventLoop();

        if (future.isSuccess()) {
            try {
                handler.channelAcquired(ch, key);
                promise.setSuccess(ch);
            } catch (Throwable cause) {
                ch.close();
                promise.setFailure(cause);
            }
        } else {
            ch.close();
            acquire(key, promise);
        }
    }

    private void newChannel(
            final K key, final Promise<C> promise) {
        Bootstrap bs = bootstrap.clone(loop(key));
        ChannelFuture f = bs.attr(KEY, key).connect(key.remoteAddress());
        if (f.isDone()) {
            notifyConnect(f, key, promise);
        } else {
            f.addListener(new ChannelFutureListener() {
                @SuppressWarnings("unchecked")
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    notifyConnect(future, key, promise);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    private void notifyConnect(ChannelFuture future, ChannelPoolKey key, Promise<C> promise) {
        if (future.isSuccess()) {
            C ch = (C) future.channel();
            ch.attr(KEY).set(key);
            promise.setSuccess(ch);
        } else {
            promise.setFailure(future.cause());
        }
    }

    @Override
    public final Future<Boolean> release(C channel) {
        return release(channel, channel.eventLoop().<Boolean>newPromise());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<Boolean> release(final C channel, final Promise<Boolean> promise) {
        checkNotNull(channel, "channel");
        checkNotNull(promise, "promise");
        try {
            final K key = (K) channel.attr(KEY).getAndSet(null);
            if (key != null) {
                ChannelPoolSegment<C> channels = pool.get(key);
                if (channels == null) {
                    channels = segmentFactory.newSegment();
                    ChannelPoolSegment<C> old = pool.putIfAbsent(key, channels);
                    if (old != null) {
                        channels = old;
                    }
                }
                final ChannelPoolSegment<C> channelQueue = channels;

                EventLoop loop = channel.eventLoop();
                if (loop.inEventLoop()) {
                    doReleaseChannel(channelQueue, key, channel, promise);
                } else {
                    loop.execute(new OneTimeTask() {
                        @Override
                        public void run() {
                            doReleaseChannel(channelQueue, key, channel, promise);
                        }
                    });
                }
            } else {
                promise.setSuccess(Boolean.FALSE);
            }

        } catch (Throwable cause) {
            promise.setFailure(cause);
            channel.close();
        }
        return promise;
    }

    private void doReleaseChannel(ChannelPoolSegment<C> channels, K key, C channel, Promise<Boolean> promise) {
        assert channel.eventLoop().inEventLoop();

        try {
            if (channels.offer(channel)) {
                handler.channelReleased(channel, key);
                promise.setSuccess(Boolean.TRUE);
            } else {
                promise.setSuccess(Boolean.FALSE);
            }
        } catch (Throwable cause) {
            channel.close();
            promise.setFailure(cause);
        }
    }
}
