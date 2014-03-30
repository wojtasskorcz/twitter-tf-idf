package to.us.bachor.iosr.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("rawtypes" /* Storm has no generic types */)
public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	LinkedBlockingQueue<Status> queue = null;
	TwitterStream twitterStream;
	String[] trackTerms;
	long maxQueueDepth;
	SpoutOutputCollector collector;

	public TwitterSpout(String[] trackTerms, long maxQueueDepth) {
		this.trackTerms = trackTerms;
		this.maxQueueDepth = maxQueueDepth;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		System.out.println("opening");
		this.collector = collector;
		queue = new LinkedBlockingQueue<Status>(1000);

		StatusListener listener = new StatusListener() {
			@Override
			public void onException(Exception arg0) {
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
			}

			@Override
			public void onStatus(Status status) {
				if (queue.size() < maxQueueDepth) {
					System.out.println("TWEET Received: " + status);
					queue.offer(status);
				} else {
					System.out.println("Queue is now full, the following message is dropped: " + status);
				}
			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
			}
		};

		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(listener);
		FilterQuery filter = new FilterQuery();
		filter.count(0);
		filter.track(trackTerms);
		twitterStream.filter(filter);
		System.out.println("end opening");
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}
		} else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}