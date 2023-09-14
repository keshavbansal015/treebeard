package site.ycsb.db;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.57.0)",
    comments = "Source: router.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class RouterGrpc {

  private RouterGrpc() {}

  public static final java.lang.String SERVICE_NAME = "router.Router";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<site.ycsb.db.ReadRequest,
      site.ycsb.db.ReadReply> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Read",
      requestType = site.ycsb.db.ReadRequest.class,
      responseType = site.ycsb.db.ReadReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<site.ycsb.db.ReadRequest,
      site.ycsb.db.ReadReply> getReadMethod() {
    io.grpc.MethodDescriptor<site.ycsb.db.ReadRequest, site.ycsb.db.ReadReply> getReadMethod;
    if ((getReadMethod = RouterGrpc.getReadMethod) == null) {
      synchronized (RouterGrpc.class) {
        if ((getReadMethod = RouterGrpc.getReadMethod) == null) {
          RouterGrpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<site.ycsb.db.ReadRequest, site.ycsb.db.ReadReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  site.ycsb.db.ReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  site.ycsb.db.ReadReply.getDefaultInstance()))
              .setSchemaDescriptor(new RouterMethodDescriptorSupplier("Read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<site.ycsb.db.WriteRequest,
      site.ycsb.db.WriteReply> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Write",
      requestType = site.ycsb.db.WriteRequest.class,
      responseType = site.ycsb.db.WriteReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<site.ycsb.db.WriteRequest,
      site.ycsb.db.WriteReply> getWriteMethod() {
    io.grpc.MethodDescriptor<site.ycsb.db.WriteRequest, site.ycsb.db.WriteReply> getWriteMethod;
    if ((getWriteMethod = RouterGrpc.getWriteMethod) == null) {
      synchronized (RouterGrpc.class) {
        if ((getWriteMethod = RouterGrpc.getWriteMethod) == null) {
          RouterGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<site.ycsb.db.WriteRequest, site.ycsb.db.WriteReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  site.ycsb.db.WriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  site.ycsb.db.WriteReply.getDefaultInstance()))
              .setSchemaDescriptor(new RouterMethodDescriptorSupplier("Write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RouterStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RouterStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RouterStub>() {
        @java.lang.Override
        public RouterStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RouterStub(channel, callOptions);
        }
      };
    return RouterStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RouterBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RouterBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RouterBlockingStub>() {
        @java.lang.Override
        public RouterBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RouterBlockingStub(channel, callOptions);
        }
      };
    return RouterBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RouterFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RouterFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RouterFutureStub>() {
        @java.lang.Override
        public RouterFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RouterFutureStub(channel, callOptions);
        }
      };
    return RouterFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void read(site.ycsb.db.ReadRequest request,
        io.grpc.stub.StreamObserver<site.ycsb.db.ReadReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }

    /**
     */
    default void write(site.ycsb.db.WriteRequest request,
        io.grpc.stub.StreamObserver<site.ycsb.db.WriteReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Router.
   */
  public static abstract class RouterImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RouterGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Router.
   */
  public static final class RouterStub
      extends io.grpc.stub.AbstractAsyncStub<RouterStub> {
    private RouterStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RouterStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RouterStub(channel, callOptions);
    }

    /**
     */
    public void read(site.ycsb.db.ReadRequest request,
        io.grpc.stub.StreamObserver<site.ycsb.db.ReadReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void write(site.ycsb.db.WriteRequest request,
        io.grpc.stub.StreamObserver<site.ycsb.db.WriteReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Router.
   */
  public static final class RouterBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RouterBlockingStub> {
    private RouterBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RouterBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RouterBlockingStub(channel, callOptions);
    }

    /**
     */
    public site.ycsb.db.ReadReply read(site.ycsb.db.ReadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public site.ycsb.db.WriteReply write(site.ycsb.db.WriteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Router.
   */
  public static final class RouterFutureStub
      extends io.grpc.stub.AbstractFutureStub<RouterFutureStub> {
    private RouterFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RouterFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RouterFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<site.ycsb.db.ReadReply> read(
        site.ycsb.db.ReadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<site.ycsb.db.WriteReply> write(
        site.ycsb.db.WriteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_READ = 0;
  private static final int METHODID_WRITE = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_READ:
          serviceImpl.read((site.ycsb.db.ReadRequest) request,
              (io.grpc.stub.StreamObserver<site.ycsb.db.ReadReply>) responseObserver);
          break;
        case METHODID_WRITE:
          serviceImpl.write((site.ycsb.db.WriteRequest) request,
              (io.grpc.stub.StreamObserver<site.ycsb.db.WriteReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getReadMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              site.ycsb.db.ReadRequest,
              site.ycsb.db.ReadReply>(
                service, METHODID_READ)))
        .addMethod(
          getWriteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              site.ycsb.db.WriteRequest,
              site.ycsb.db.WriteReply>(
                service, METHODID_WRITE)))
        .build();
  }

  private static abstract class RouterBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RouterBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return site.ycsb.db.RouterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Router");
    }
  }

  private static final class RouterFileDescriptorSupplier
      extends RouterBaseDescriptorSupplier {
    RouterFileDescriptorSupplier() {}
  }

  private static final class RouterMethodDescriptorSupplier
      extends RouterBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RouterMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RouterGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RouterFileDescriptorSupplier())
              .addMethod(getReadMethod())
              .addMethod(getWriteMethod())
              .build();
        }
      }
    }
    return result;
  }
}
