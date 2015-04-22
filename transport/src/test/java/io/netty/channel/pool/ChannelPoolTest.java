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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ChannelPoolTest {
    private static final String LOCAL_ADDR_ID = "test.id";

    @Test
    public void testPool() throws Exception {
        EventLoopGroup group = new LocalEventLoopGroup();
        LocalAddress addr = new LocalAddress(LOCAL_ADDR_ID);
        Bootstrap cb = new Bootstrap();
        ServerBootstrap sb = new ServerBootstrap();

        cb.group(group)
          .channel(LocalChannel.class);

        sb.group(group)
          .channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
              @Override
              public void initChannel(LocalChannel ch) throws Exception {
                  ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
              }
          });

        // Start server
        Channel sc = sb.bind(addr).sync().channel();
        final AtomicInteger channelCount = new AtomicInteger(0);
        final AtomicInteger acquiredCount = new AtomicInteger(0);
        final AtomicInteger releasedCount = new AtomicInteger(0);

        ChannelPool<Channel, ChannelPoolKey> pool =
                new SimpleChannelPool<Channel, ChannelPoolKey>(cb, new ChannelPoolHandler<Channel, ChannelPoolKey>() {
            @Override
            public void channelCreated(Channel ch, ChannelPoolKey key) {
                channelCount.incrementAndGet();
            }

            @Override
            public void channelReleased(@SuppressWarnings("unused") Channel ch,
                                        @SuppressWarnings("unused") ChannelPoolKey key) {
                releasedCount.incrementAndGet();
            }

            @Override
            public void channelAcquired(@SuppressWarnings("unused") Channel ch,
                                        @SuppressWarnings("unused") ChannelPoolKey key) {
                acquiredCount.incrementAndGet();
            }
        });

        ChannelPoolKey key = new DefaultChannelPoolKey(addr);
        ChannelPoolKey key2 = new DefaultChannelPoolKey(addr, group.next());

        Channel channel = pool.acquire(key).sync().getNow();

        Assert.assertTrue(pool.release(channel).syncUninterruptibly().getNow());

        Channel channel2 = pool.acquire(key).sync().getNow();
        Assert.assertSame(channel, channel2);
        Assert.assertEquals(1, channelCount.get());

        Channel channel3 = pool.acquire(key2).sync().getNow();
        Assert.assertNotSame(channel, channel3);
        Assert.assertEquals(2, channelCount.get());
        Assert.assertTrue(pool.release(channel3).syncUninterruptibly().getNow());

        // Should fail on multiple release calls.
        Assert.assertFalse(pool.release(channel3).syncUninterruptibly().getNow());

        Assert.assertEquals(1, acquiredCount.get());
        Assert.assertEquals(2, releasedCount.get());

        sc.close().sync();
        channel2.close().sync();
        channel3.close().sync();
        group.shutdownGracefully();
    }

    @Test
    public void testFixedPool() throws Exception {
        EventLoopGroup group = new LocalEventLoopGroup();
        LocalAddress addr = new LocalAddress(LOCAL_ADDR_ID);
        Bootstrap cb = new Bootstrap();
        ServerBootstrap sb = new ServerBootstrap();

        cb.group(group)
          .channel(LocalChannel.class);

        sb.group(group)
          .channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
              @Override
              public void initChannel(LocalChannel ch) throws Exception {
                  ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
              }
          });

        // Start server
        Channel sc = sb.bind(addr).sync().channel();
        final AtomicInteger channelCount = new AtomicInteger(0);
        final AtomicInteger acquiredCount = new AtomicInteger(0);
        final AtomicInteger releasedCount = new AtomicInteger(0);

        ChannelPool<Channel, ChannelPoolKey> pool
                = new FixedChannelPool<Channel, ChannelPoolKey>(cb, new ChannelPoolHandler<Channel, ChannelPoolKey>() {
                    @Override
                    public void channelCreated(Channel ch, ChannelPoolKey key) {
                        channelCount.incrementAndGet();
                    }

                    @Override
                    public void channelReleased(@SuppressWarnings("unused") Channel ch,
                                                @SuppressWarnings("unused") ChannelPoolKey key) {
                        releasedCount.incrementAndGet();
                    }

                    @Override
                    public void channelAcquired(@SuppressWarnings("unused") Channel ch,
                                                @SuppressWarnings("unused") ChannelPoolKey key) {
                        acquiredCount.incrementAndGet();
                    }
                }, 1, Integer.MAX_VALUE);

        ChannelPoolKey key = new DefaultChannelPoolKey(addr);
        Channel channel = pool.acquire(key).sync().getNow();
        Future<Channel> future = pool.acquire(key);
        Assert.assertFalse(future.isDone());
        Assert.assertTrue(pool.release(channel).syncUninterruptibly().getNow());
        Assert.assertTrue(future.await(1, TimeUnit.SECONDS));

        Channel channel2 = future.getNow();
        Assert.assertSame(channel, channel2);
        Assert.assertEquals(1, channelCount.get());

        Assert.assertEquals(1, acquiredCount.get());
        Assert.assertEquals(1, releasedCount.get());

        sc.close().sync();
        channel2.close().sync();
        group.shutdownGracefully();
    }
}
