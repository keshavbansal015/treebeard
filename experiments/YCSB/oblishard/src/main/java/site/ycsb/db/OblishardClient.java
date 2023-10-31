package site.ycsb.db;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.RouterGrpc.RouterBlockingStub;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class OblishardClient extends DB {
  private RouterBlockingStub stub;

  @Override
  public void init() throws DBException {
    String host = "127.0.0.1";
    int port = 8745;
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    stub = RouterGrpc.newBlockingStub(channel);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    ReadRequest req = ReadRequest.newBuilder().setBlock(key).build();
    try {
      ReadReply reply = stub.read(req);
      System.out.println(reply);
      return Status.OK;
    } catch (StatusRuntimeException e) {
      System.out.println(e.toString());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    WriteRequest req = WriteRequest.newBuilder().setBlock(key).setValue(values.get("data").toString()).build();
    try {
      WriteReply reply = stub.write(req);
      System.out.println(reply);
      return Status.OK;
    } catch (StatusRuntimeException e) {
      System.out.println(e.toString());
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.ERROR;
  }
}