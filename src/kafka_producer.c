#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

static int run = 1;

static void stop(int signal) {
  run = 0;
  fclose(stdin);
}

/* 
 * Callback for delivered message
 */
static void delivery_message_callback(rd_kafka_t *rk_handler,
				      const rd_kafka_message_t *rk_message, void *opaque) {
  if(rk_message->err) {
    fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rk_message->err));
  } else {
    fprintf(stdout, "%% Message delivered (%zd bytes, ""partition %"PRId32")\n",
	    rk_message->len, rk_message->partition);
  }
  
}

int main() {

  rd_kafka_t *rk_handler; // instance handler
  rd_kafka_topic_t *rk_topic; // Topic object
  rd_kafka_conf_t *conf;
  const char *broker = "localhost:9092"; // broker list
  const char *topic = "test"; // topic to produce to
  char errstr[512];  // reporting buffer
  char buff[512]; // message value buffer

  printf("Starting Program...\n");
  conf = rd_kafka_conf_new();

  if (rd_kafka_conf_set(conf, "bootstrap.servers", broker, errstr,
			sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "%s\n", errstr);
    return 1;
  }

  rd_kafka_conf_set_dr_msg_cb(conf, delivery_message_callback);
  rk_handler = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if(!rk_handler) {
    fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
    return 1;
  }

  rk_topic = rd_kafka_topic_new(rk_handler, topic, NULL);
  if(!rk_topic) {
    fprintf(stderr, "%% Failed to create topic object: %s\n",
	    rd_kafka_err2str(rd_kafka_last_error()));
    rd_kafka_destroy(rk_handler);

    return 1;
  }

  signal(SIGINT, stop);

  fprintf(stderr,
	  "%% Type some text and hit enter to produce message\n"
	  "%% Or just hit enter to only serve delivery reports\n"
	  "%% Press Ctrl-C or Ctrl-D to exit\n");

  while (run && fgets(buff, sizeof(buff), stdin)) {
    size_t len = strlen(buff);

    if(buff[len-1] == '\n')
      buff[--len] = '\0';

    if (len == 0) {
      rd_kafka_poll(rk_handler, 0);
      continue;
    }

  retry:
    if (rd_kafka_produce(rk_topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
			 buff, len, NULL, 0, NULL) == -1) {
      fprintf(stderr, "%% Failed to produce to topic %s: %s\n",
	      rd_kafka_topic_name(rk_topic),
	      rd_kafka_err2str(rd_kafka_last_error()));
      if(rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
	rd_kafka_poll(rk_handler, 1000);
	goto retry;
      }
      
    } else {
      fprintf(stderr, "%% Enqueued message (%zd bytes) "
	     "for topic %s\n",
	     len, rd_kafka_topic_name(rk_topic));
    }

    rd_kafka_poll(rk_handler, 0);
  }
  
  rd_kafka_flush(rk_handler, 10*1000);
  rd_kafka_topic_destroy(rk_topic);
  rd_kafka_destroy(rk_handler);
  return 0;
}
