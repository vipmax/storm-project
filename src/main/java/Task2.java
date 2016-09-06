import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.*;
import java.util.*;

/**
 * Created by farid on 6/15/2016.
 */
public class Task2 {
    final static String tube = "word";

    public static class MySpout extends BaseRichSpout {
        SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            final Random rand = new Random();

            String s = "";
            for (int i = 0; i < 10000; i++) {
                s += rand.nextInt(100) + "";
            }

            collector.emit(new Values(s));

            Utils.sleep(500);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(tube));
        }
    }

    public static class MyBolt extends BaseRichBolt {
        // megabytes
        final static int N = 1;

        private int componentId;

        long counter = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            componentId = context.getThisTaskId();
        }

        @Override
        public void execute(Tuple tuple) {
            String value = tuple.getStringByField(tube);

            try {
                int length =  value.getBytes("UTF-8").length;
                counter += length;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            float size = (float) counter / (1024 * 1024);
            System.out.println("componentId = " + componentId + " size = " + size);
            appendToFile(size);
            if (size > N) {
                System.out.println(N + " megabyte(s) is(are) processed");
                counter = 0;
            }
        }

        private void appendToFile(float size) {
            try {
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("outfile2.txt", true)));
                out.println("componentId = " + componentId + " size = " + size);
                if (size > 1) out.println("componentId = " + componentId + " " + N +" megabyte(s) is(are) processed");
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(tube));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(tube, new MySpout(), 4);
        builder.setBolt("", new MyBolt(), 2).shuffleGrouping(tube);

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
    }
}
