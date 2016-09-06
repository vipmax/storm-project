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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by farid on 6/15/2016.
 */
public class Task1 {
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
            int value = rand.nextInt(100);
            collector.emit(new Values(value));

            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(tube));
        }
    }

    public static class MyBolt extends BaseRichBolt {

        List<Integer> numbers = new ArrayList<>();
        final static int N = 2;
        private int componentId;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            componentId = context.getThisTaskId();
        }

        @Override
        public void execute(Tuple tuple) {
            Integer value = tuple.getIntegerByField(tube);
            numbers.add(value);

            if (numbers.size() >= N) {
                // sorting
                Collections.sort(numbers);

                System.out.println("ComponentId = " + componentId + " buffer = " + numbers);

//                appendToFile();

                numbers.clear();
            }
        }

        private void appendToFile() {
            try {
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("outfile.txt", true)));
                out.println("ComponentId = " + componentId + " buffer = " + numbers);
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

        builder.setSpout(tube, new MySpout(), 2);
        builder.setBolt("", new MyBolt(), 4).shuffleGrouping(tube);

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
    }
}
