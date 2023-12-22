package ai.bsdb.tools;

import ai.bsdb.read.AsyncReader;
import ai.bsdb.read.SyncReader;
import ai.bsdb.serde.Field;
import ai.bsdb.serde.JsonDeser;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.cli.*;
import org.msgpack.core.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.CompletionHandler;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.*;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class HttpServer {
    private static final int SO_BACKLOG_VALUE = 128;
    private static final int WORKER_THREADS_COUNT = Runtime.getRuntime().availableProcessors();

    static Logger logger = LoggerFactory.getLogger(HttpServer.class);

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        //boolean verbose = cmd.hasOption("v");
        //boolean compress = cmd.hasOption("z");
        boolean async = cmd.hasOption("async");
        boolean toJson = cmd.hasOption("json");

        String dataDir = cmd.getOptionValue("d", "./rdb");
        String address = cmd.getOptionValue("A", "0.0.0.0");
        String port = cmd.getOptionValue("p", "9999");

        String prefix = cmd.getOptionValue("P", "/bsdb/");

        String threads = cmd.getOptionValue("t", String.valueOf(WORKER_THREADS_COUNT));
        //int compressBlockSize = Integer.parseInt(cmd.getOptionValue("bs", "" + Common.DEFAULT_COMPRESS_BLOCK_SIZE));

        SyncReader db = null;
        AsyncReader asyncDB = null;
        if (async) {
            asyncDB = new AsyncReader(new File(dataDir), cmd.hasOption('a'), cmd.hasOption("id"), cmd.hasOption("kd"));
        } else {
            db = new SyncReader(new File(dataDir), cmd.hasOption("ic"), cmd.hasOption('a'), cmd.hasOption("id"), cmd.hasOption("kd"));
        }

        startHttpServer(InetAddress.getByName(address), Integer.parseInt(port), Integer.parseInt(threads), db, asyncDB, prefix, async, toJson);
    }


    private static CommandLine parseArgs(String[] args) {
        Options options = new Options();
        //options.addOption("v", "verbose", false, "Enable verbose mode");
        options.addOption("ic", "cache-index", false, "Hold index in memory");
        //options.addOption("z", "compressed", false, "DB records compressed");
        options.addOption("A", "address", true, "Specify http listen address, default to 0.0.0.0");
        options.addOption("p", "port", true, "Specify http listen port, default to 9999");
        options.addOption("d", "dir", true, "Specify data directory, default to ./rdb");
        options.addOption("P", "prefix", true, "Specify http uri prefix, default to /bsdb/");
        options.addOption("t", "threads", true, "Specify worker thread number, default to processor count\" \"");
        options.addOption("a", "approximate", false, "Approximate mode, keys will not be stored, choosing proper checksum bits to meet false-positive query rate");
        //options.addOption("bs", "compress-block-size", true, "Block size for compression, default 1024");
        options.addOption("id", "index-direct-io", false, "use o_direct to read index");
        options.addOption("kd", "kv-direct-io", false, "use o_direct to read kv");
        options.addOption("async", "async", false, "use async mode to query db");
        options.addOption("json", "json-output", false, "deserialize msgpack values to json output");

        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Error parsing input args", e);
            System.exit(1);
            return null;
        }
    }

    static void startHttpServer(InetAddress address, int port, int threadCount, final SyncReader db, final AsyncReader asyncDB, String prefix, boolean async, boolean toJson) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        final EventExecutorGroup mainGroup = new DefaultEventExecutorGroup(threadCount);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new HttpServerInitializer(mainGroup, db, asyncDB, prefix, async, toJson));

            bootstrap.option(ChannelOption.SO_BACKLOG, SO_BACKLOG_VALUE);
            bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture future = bootstrap.bind(address, port).sync();
            logger.info("Http server started on {}:{}", address.getHostAddress(), port);
            future.channel().closeFuture().sync();
        } finally {
            mainGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    static class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
        SyncReader db;
        AsyncReader asyncDB;
        String prefix;
        EventExecutorGroup executor;
        boolean async;
        boolean toJson;

        HttpServerInitializer(EventExecutorGroup executor, SyncReader db, AsyncReader asyncDB, String prefix, boolean async, boolean toJson) {
            this.db = db;
            this.asyncDB = asyncDB;
            this.prefix = prefix;
            this.executor = executor;
            this.async = async;
            this.toJson = toJson;
        }

        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(executor, new HttpServerCodec());
            //p.addLast(new HttpContentCompressor((CompressionOptions[]) null));
            p.addLast(executor, new HttpServerExpectContinueHandler());
            p.addLast(executor, new HttpServerHandler(this.db, this.asyncDB, this.prefix, async, toJson));
        }
    }


    static class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {
        AsyncReader asyncDB;
        SyncReader db;
        String prefix;
        int keyPosInUri;
        boolean async;
        boolean toJson;
        Field[] valueSchame;
        JsonDeser jsonDser;

        HttpServerHandler(SyncReader db, AsyncReader asyncDB, String prefix, boolean async, boolean toJson) {
            this.db = db;
            this.asyncDB = asyncDB;
            this.prefix = prefix;
            this.async = async;

            this.keyPosInUri = prefix.length();

            if (toJson) {
                this.valueSchame = db.getValueSchema();
                if (this.valueSchame != null) {
                    this.toJson = toJson;
                    this.jsonDser = new JsonDeser();
                }
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;
                String key = req.uri().substring(this.keyPosInUri);

                if (async) {
                    try {
                        this.asyncDB.asyncGet(key.getBytes(), null, new CompletionHandler<byte[], Object>() {
                            @Override
                            public void completed(byte[] value, Object o) {
                                try {
                                    byte[] encoded = encodeResponse(value);
                                    sendSuccessResp(ctx, req, encoded);
                                } catch (IOException e) {
                                    logger.error("encode response failed", e);
                                    sendErrorResp(ctx, req, e.getMessage());
                                }
                            }

                            @Override
                            public void failed(Throwable throwable, Object o) {
                                logger.error("async get KV failed", throwable);
                                sendErrorResp(ctx, req, throwable.getMessage());
                            }
                        });
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    try {
                        byte[] value = this.db.getAsBytes(key.getBytes());
                        byte[] encoded = encodeResponse(value);
                        sendSuccessResp(ctx, req, encoded);
                    } catch (Exception e) {
                        logger.error("encode response failed", e);
                        sendErrorResp(ctx, req, e.getMessage());
                    }
                }

            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        private void sendSuccessResp(ChannelHandlerContext ctx, HttpRequest req, byte[] content) {

            boolean keepAlive = HttpUtil.isKeepAlive(req);
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    content == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(content));
            response.headers()
                    .set(CONTENT_TYPE, TEXT_PLAIN)
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());

            if (keepAlive) {
                if (!req.protocolVersion().isKeepAliveDefault()) {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                }
            } else {
                // Tell the client we're going to close the connection.
                response.headers().set(CONNECTION, CLOSE);
            }

            ChannelFuture f = ctx.writeAndFlush(response);

            if (!keepAlive) {
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }

        private byte[] encodeResponse(byte[] content) throws IOException {
            if (content != null && toJson) {
                return this.jsonDser.from(MessagePack.newDefaultUnpacker(content), this.valueSchame);
            } else {
                return content;
            }
        }

        private void sendErrorResp(ChannelHandlerContext ctx, HttpRequest req, String errMsg) {
            boolean keepAlive = HttpUtil.isKeepAlive(req);
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), INTERNAL_SERVER_ERROR,
                    errMsg == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(errMsg.getBytes()));
            response.headers()
                    .set(CONTENT_TYPE, TEXT_PLAIN)
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());

            if (keepAlive) {
                if (!req.protocolVersion().isKeepAliveDefault()) {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                }
            } else {
                // Tell the client we're going to close the connection.
                response.headers().set(CONNECTION, CLOSE);
            }

            ChannelFuture f = ctx.write(response);

            if (!keepAlive) {
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
