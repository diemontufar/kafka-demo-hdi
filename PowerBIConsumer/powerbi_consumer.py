# invoke with: python powerbi_consumer.py -f ./confluent_python.config -t siniestros
import json
import requests
from confluent_kafka import Consumer
import ccloud_lib

def publishMessageToPBI(message):
    
    url = "https://api.powerbi.com/beta/ee671ff4-00bc-4321-b324-449896173882/datasets/622018e6-79f9-4c37-bbe8-6286e3a4e334/rows?key=0Ns4gS9TQWgZmCIDaEgJSOedJ39wWG3IB5JUeUcFPaltuVgEhTBnv2QG93NPkj8uWmRUQnD52AHe5eq%2Ft7AJHQ%3D%3D"

    data=json.loads(message)
    data=[data]
    
    response = requests.post(url,json=data)
    print(response.status_code, response.reason, "message: " + str(data))


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'hdiseguros-group-4'
    consumer_conf['auto.offset.reset'] = 'earliest'
    #consumer_conf['auto.offset.reset'] = 'latest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                
                publishMessageToPBI(record_value)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()



