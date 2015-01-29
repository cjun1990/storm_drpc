package com.ck;

import java.util.Map;
import java.util.Random;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MyLogSpout implements ITridentSpout<MyMetadata> {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  /**
   * @fieldName: _cycle
   * @fieldType: boolean
   * @Description: 是否循环发送数据
   */
  private boolean _cycle;

  /**
   * @fieldName: _offset
   * @fieldType: int
   * @Description: 数据发送的偏移量,每批次处理10条
   */
  private static final int _offset = 10;

  private Fields _fields;

  public MyLogSpout() {}

  public MyLogSpout(Fields fields, boolean cycle) {
    this._fields = fields;
    this._cycle = cycle;
  }

  @Override
  public BatchCoordinator<MyMetadata> getCoordinator(String txStateId, Map conf,
      TopologyContext context) {
    return new MyCoordinator();
  }

  @Override
  public Emitter<MyMetadata> getEmitter(String txStateId, Map conf, TopologyContext context) {
    return new MyEmitter();
  }

  @Override
  public Map getComponentConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Fields getOutputFields() {
    return _fields;
  }

  /**
   * @author xuer
   * @date 2014-9-19 - 上午10:31:56
   * @Description spout的Coordinator
   */
  public class MyCoordinator implements ITridentSpout.BatchCoordinator<MyMetadata> {

    /**
     * @Title: initializeTransaction
     * @Description: TODO
     * @param txid
     * @param prevMetadata
     * @param currMetadata
     * @return 当前事务处理的元数据
     */
    @Override
    public MyMetadata initializeTransaction(long txid, MyMetadata prevMetadata,
        MyMetadata currMetadata) {
      if (prevMetadata == null || prevMetadata.getIndex() <= 0) {
        currMetadata = new MyMetadata();
        currMetadata.setIndex(0);
        currMetadata.setOffset(_offset);
      } else {
        long newIndex = prevMetadata.getIndex() + prevMetadata.getOffset();
        currMetadata.setIndex(newIndex);
        currMetadata.setOffset(_offset);
      }
      return currMetadata;
    }

    @Override
    public void success(long txid) {
      // TODO Auto-generated method stub
    }

    @Override
    public boolean isReady(long txid) {
      return true;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
    }

  }

  /**
   * @author xuer
   * @date 2014-9-19 - 上午10:42:28
   * @Description spout的emitter
   */
  public class MyEmitter implements ITridentSpout.Emitter<MyMetadata> {

    @Override
    public void emitBatch(TransactionAttempt tx, MyMetadata coordinatorMeta,
        TridentCollector collector) {
      for (int i = 0; i < coordinatorMeta.getOffset(); i++) {
        collector.emit(new Values(radamDateLog()));
      }
    }

    @Override
    public void success(TransactionAttempt tx) {
      // TODO Auto-generated method stub
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
    }
  }

  public boolean is_cycle() {
    return _cycle;
  }

  public void set_cycle(boolean _cycle) {
    this._cycle = _cycle;
  }

  public int get_offset() {
    return _offset;
  }

  /**
   * @Title: radamDateLog
   * @Description: TODO
   * @return
   * @return: 生成随机日期的函数
   */
  public String radamDateLog() {
    String[] date =
        {"20140101000000", "20140102000000", "20140103000000", "20140104000000", "20140105000000",
            "20140106000000", "20140107000000", "20140108000000", "20140109000000"};

    Random random = new Random();

    String log =
        "BL##ERROR#"
            + date[random.nextInt(9)]
            + "#cmszmonc#pboss#mon#upay_monAbnormalPay.sh##upay_monAbnormalPay#00000000#0###tttt##LB";

    return log;
  }

  public Fields get_fields() {
    return _fields;
  }

  public void set_fields(Fields _fields) {
    this._fields = _fields;
  }
}
