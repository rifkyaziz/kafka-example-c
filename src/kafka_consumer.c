#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

int run = 1;
int exit_eof = 0;
static void consume(rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err) {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            fprintf(stderr,
                    "%% Consumer reached end of %s [%"PRId32"] "
                    "message queue at offset %"PRId64"\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition, rkmessage->offset);
            if(exit_eof)
                run = 0;
            return;
        }

        fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
                        "offset %"PRId64": %s\n",
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition,
                rkmessage->offset,
                rd_kafka_message_errstr(rkmessage));

        if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
            rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
            run = 0;
        return;
    }

    if (rkmessage->key_len) {
        printf("Key: %.*s\n", (int)rkmessage->key_len, (char *)rkmessage->key);
    }

    printf("> %.*s\n",
           (int)rkmessage->len, (char *)rkmessage->payload);
}

int main() {
    rd_kafka_t *rk_handler; // instance handler
    rd_kafka_topic_t *rk_topic; // Topic object
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    const char *broker = "localhost:9092"; // broker list
    const char *topic = "test"; // topic to produce to
    char errstr[512];  // reporting buffer

    printf("Starting Program...\n");
    conf = rd_kafka_conf_new();
    topic_conf = rd_kafka_topic_conf_new();

    rd_kafka_topic_conf_set(topic_conf, "topic", topic, errstr, sizeof(errstr));

    if (rd_kafka_conf_set(conf, "bootstrap.servers", broker, errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    rk_handler = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if(!rk_handler) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
        return 1;
    }

    /* Add brokers */
    if (rd_kafka_brokers_add(rk_handler, broker) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }

    /* Create topic */
    rk_topic = rd_kafka_topic_new(rk_handler, topic, topic_conf);
    topic_conf = NULL; /* Now owned by topic */

    /* Start consuming */
    if (rd_kafka_consume_start(rk_topic, 0, 0) == -1){
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        fprintf(stderr, "%% Failed to start consuming: %s\n",
                rd_kafka_err2str(err));
        if (err == RD_KAFKA_RESP_ERR__INVALID_ARG)
            fprintf(stderr,
                    "%% Broker based offset storage "
                    "requires a group.id, "
                    "add: -X group.id=yourGroup\n");
        exit(1);
    }

    while(run) {
        rd_kafka_message_t *rkmessage;

        /* Poll for errors, etc. */
        rd_kafka_poll(rk_handler, 0);

        /* Consume single message.
         * See rdkafka_performance.c for high speed
         * consuming of messages. */
        rkmessage = rd_kafka_consume(rk_topic, 0, 1000);
        if (!rkmessage) /* timeout */
            continue;

        consume(rkmessage, NULL);

        /* Return message to rdkafka */
        rd_kafka_message_destroy(rkmessage);
    }

    /* Stop consuming */
    rd_kafka_consume_stop(rk_topic, 1);

    while (rd_kafka_outq_len(rk_handler) > 0)
        rd_kafka_poll(rk_handler, 10);

    /* Destroy topic */
    rd_kafka_topic_destroy(rk_topic);

    /* Destroy handle */
    rd_kafka_destroy(rk_handler);

    return 0;
}
