import { Kafka, type ConsumerConfig, type ConsumerSubscribeTopics, type EachMessageHandler } from  'kafkajs'

interface KafkaClientWrapperI {
    listen(config: ConsumerConfig, subscribeConfig: ConsumerSubscribeTopics,runner: EachMessageHandler):void
}

const newKafkaWrapper = (clientId: string,brokers: string[]): KafkaClientWrapperI => {
    return new KafkaClientWrapper(clientId, brokers);
}

class KafkaClientWrapper implements KafkaClientWrapperI{

    private clientId: string
    private brokers: string[]
    private client: Kafka

    constructor(clientId: string,brokers: string[]){
        this.clientId = clientId
        this.brokers = brokers
        this.client = new Kafka({
            clientId, 
            brokers
        })
        
    }
    listen = async (config: ConsumerConfig, subscribeConfig: ConsumerSubscribeTopics,runner: EachMessageHandler)=>{
        const consumer = this.client.consumer(config)
        consumer.connect()
        consumer.subscribe(subscribeConfig)
        await consumer.run({
            eachMessage: runner
        })
    }
}

export {
    newKafkaWrapper
};
export type { KafkaClientWrapperI };
