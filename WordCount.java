package org.virginia.do5xb.twitter;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;

// For logging
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

//There are a variety of bolt types. In this case, use BaseBasicBolt
public class WordCount extends BaseBasicBolt {
  //Create logger for this class
  private static final Logger logger = LogManager.getLogger(WordCount.class);
  //For holding words and counts
  Map<String, Integer> counts = new HashMap<String, Integer>();
  //How often to emit a count of words
  private Integer emitFrequency;
  private String[] stopwords={"a",
				"about",
				"above",
				"after",
				"again",
				"against",
				"all",
				"am",
				"an",
				"and",
				"any",
				"are",
				"aren't",
				"as",
				"at",
				"be",
				"because",
				"been",
				"before",
				"being",
				"below",
				"between",
				"both",
				"but",
				"by",
				"can't",
				"cannot",
				"could",
				"couldn't",
				"did",
				"didn't",
				"do",
				"does",
				"doesn't",
				"doing",
				"don't",
				"down",
				"during",
				"each",
				"few",
				"for",
				"from",
				"further",
				"had",
				"hadn't",
				"has",
				"hasn't",
				"have",
				"haven't",
				"having",
				"he",
				"he'd",
				"he'll",
				"he's",
				"her",
				"here",
				"here's",
				"hers",
				"herself",
				"him",
				"himself",
				"his",
				"how",
				"how's",
				"i",
				"i'd",
				"i'll",
				"i'm",
				"i've",
				"if",
				"in",
				"into",
				"is",
				"isn't",
				"it",
				"it's",
				"its",
				"itself",
				"let's",
				"me",
				"more",
				"most",
				"mustn't",
				"my",
				"myself",
				"no",
				"nor",
				"not",
				"of",
				"off",
				"on",
				"once",
				"only",
				"or",
				"other",
				"ought",
				"our",
				"ours",
				"ourselves",
				"out",
				"over",
				"own",
				"same",
				"shan't",
				"she",
				"she'd",
				"she'll",
				"she's",
				"should",
				"shouldn't",
				"so",
				"some",
				"such",
				"than",
				"that",
				"that's",
				"the",
				"their",
				"theirs",
				"them",
				"themselves",
				"then",
				"there",
				"there's",
				"these",
				"they",
				"they'd",
				"they'll",
				"they're",
				"they've",
				"this",
				"those",
				"through",
				"to",
				"too",
				"under",
				"until",
				"up",
				"very",
				"was",
				"wasn't",
				"we",
				"we'd",
				"we'll",
				"we're",
				"we've",
				"were",
				"weren't",
				"what",
				"what's",
				"when",
				"when's",
				"where",
				"where's",
				"which",
				"while",
				"who",
				"who's",
				"whom",
				"why",
				"why's",
				"with",
				"won't",
				"would",
				"wouldn't",
				"you",
				"you'd",
				"you'll",
				"you're",
				"you've",
				"your",
				"yours",
				"yourself",
				"yourselves"};
	
  // Default constructor
  public WordCount() {
      emitFrequency=30; // Default to 60 seconds
  }

  // Constructor that sets emit frequency
  public WordCount(Integer frequency) {
      emitFrequency=30;
  }

  //Configure frequency of tick tuples for this bolt
  //This delivers a 'tick' tuple on a specific interval,
  //which is used to trigger certain actions
  @Override
  public Map<String, Object> getComponentConfiguration() {
      Config conf = new Config();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
      return conf;
  }

  //execute is called to process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //If it's a tick tuple, emit all words and counts
    if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
      for(String word : counts.keySet()) {
        Integer count = counts.get(word);
        collector.emit(new Values(word, count));
	if (count>=5) {
        logger.info("Emitting a count of " + count + " for word " + word);
	
	}
      }	
    } else {
      //Get the word contents from the tuple
      String word = tuple.getString(0);
      //Have we counted any already?
      Integer count = counts.get(word);
      Boolean stop=true;
      for (int i=0;i<word.length();i++){
	  char character = word.charAt(i);
	  int ascii = (int) character;
	  if ((ascii>= 97 && ascii <=122 )||(ascii>=65 && ascii <=90) || (ascii==39)){
	  } else{ stop=false; break;}
      }
      if (stop) {
      for (int i=0;i<174;i++){
	  if (word.length()==stopwords[i].length()){
              Boolean same=true;
	      for (int j=0;j<word.length();j++){
		  char c1=word.charAt(j);
		  int a1=(int) c1;
		  char c2=stopwords[i].charAt(j);
		  int a2=(int) c2;
		  if (a1!=a2 && a1!=a2-32) same=false;
	      }
	      if (same) {stop=false; break;}
	  }
      }
      }
      if (count == null)
        count = 0;
      //Increment the count and store it
      count++;
      if (stop==true) {counts.put(word, count);}
    }
  }

  //Declare that this emits a tuple containing two fields; word and count
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count"));
  }
}
