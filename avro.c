
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libserdes/serdes-avro.h>
#include "kafkacat.h"

void cnv_msg_output_avro(const rd_kafka_message_t *rkmessage, char *str) {
    serdes_t *serdes;
    serdes_err_t err;

    serdes_conf_t *sconf = serdes_conf_new(NULL, 0, "schema.registry.url", conf.sru, NULL);

    char errstr[512];
    serdes = serdes_new(sconf, errstr, sizeof(errstr));

    avro_value_t avro;
    serdes_schema_t *schema;
    char *as_json;

    if (rkmessage->payload) {
        err = serdes_deserialize_avro(serdes, &avro, &schema,
                                      rkmessage->payload, rkmessage->len,
                                      errstr, sizeof(errstr));

        if (err) {
            fprintf(stderr, "%% serdes_deserialize_avro failed: %s\n", errstr);
        }

        if (avro_value_to_json(&avro, 1, &as_json))
            fprintf(stderr, "%% avro_to_json failed: %s\n", avro_strerror());
        else {
            strcpy(str, as_json);
            free(as_json);
        }
    }
}
