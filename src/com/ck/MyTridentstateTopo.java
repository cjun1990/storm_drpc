package com.ck;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * @author xuer
 * @date 2014-9-22 - 下午2:36:27
 * @Description storm trident demo, drpc调用
 */
public class MyTridentstateTopo {
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
      InterruptedException {
    Config config = new Config();
    config.setMaxSpoutPending(20);
    if (args != null && args.length <= 0) {
      LocalCluster localCluster = new LocalCluster();
      LocalDRPC localDRPC = new LocalDRPC();
      localCluster.submitTopology("myTridentstate", config, bulidStormTopo(localDRPC));

      // 执行这里和执行topo里面是并行的，走到这里更快一点
      for (int i = 0; i < 10; i++) {
        Thread.sleep(1000);
        System.err.println(localDRPC.execute("logCount",
            "20140101000000 20140102000000 20140103000000"));
      }

    } else {
      StormSubmitter.submitTopology("myTridentstate", config, bulidStormTopo(null));
    }
  }

  public static StormTopology bulidStormTopo(LocalDRPC localDRPC) {

    TridentTopology topo = new TridentTopology();
    MyLogSpout spout = new MyLogSpout(new Fields("log"), true);

    TridentState state =
        topo.newStream("iTridentSpout", spout)
            .each(new Fields("log"), new MySplitFunction("#"), new Fields("date"))
            // each函数会把输出的字段放到输入的字段后面一起输出
            .groupBy(new Fields("date"))
            .persistentAggregate(new MemoryMapState.Factory(), new Fields("log"), new Count(),
                new Fields("count"));

    topo.newDRPCStream("logCount", localDRPC)
        .each(new Fields("args"), new MySplitFunction(" "), new Fields("date"))
        // 把传递进来的需要查询的日期先分组， 在求数量
        .groupBy(new Fields("date"))
        // 查询出每个日期的结果
        .stateQuery(state, new Fields("date"), new MapGet(), new Fields("count"))
        .each(new Fields("count"), new FilterNull())// 如果这里没有filter，那么会导致nullpointexception
        // 每个日期的结果求和
        .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

    return topo.build();
  }
}
