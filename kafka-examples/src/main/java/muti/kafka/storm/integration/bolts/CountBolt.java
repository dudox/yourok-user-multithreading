package muti.kafka.storm.integration.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CountBolt.
 * 
 * Bolt that implements logic to separate 
 * unique words and count their occurrences.
 *
 * @author Andrea Muti
 * created: 01 nov 2017
 *
 */
public class CountBolt implements IRichBolt{

	private static final long serialVersionUID = 2211212719323185944L;

	private static final Logger logger = LogManager.getLogger("FileLogger");
	

	Map<String, Integer> counters;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);

		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) +1;
			counters.put(str, c);
		}

		collector.ack(input);
	}

	@Override
	public void cleanup() {
		
		for (Map.Entry<String, Integer> entry:counters.entrySet()) {
			logger.info("Word {} - count: {}", entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}