package muti.kafka.storm.integration.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * SplitBolt.
 * 
 * Bolt that implements the logic 
 * to split a sentence into words
 * 
 * @author Andrea Muti
 * created: 01 nov 2017
 *
 */
public class SplitBolt implements IRichBolt {
	
	private static final long serialVersionUID = -6091411463225076151L;
	
	private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");

		for(String word: words) {
			word = word.trim();

			if(!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}

		collector.ack(input);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void cleanup() {}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}