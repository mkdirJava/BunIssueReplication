import { newKafkaWrapper, type KafkaClientWrapperI } from "./kafkaRunner/kafkaRunner";

const getEnvOrError = ( env_param :string): string => {
    const variable = process.env[env_param];
    if(variable === undefined){
        throw new Error(`Env not set ${env_param}`)
    }
    return variable;
}

const kafkaClientWrapper: KafkaClientWrapperI = newKafkaWrapper(
    getEnvOrError("CLIENT_ID"),
    [getEnvOrError("BROKER")]
);

kafkaClientWrapper.listen(
    { groupId: getEnvOrError("GROUP_ID") },
    { topics: [getEnvOrError("TOPIC")], fromBeginning: true },   
    async ({topic, partition,message})=>{
        console.log(`${topic}, ${partition}, ${message}`)
    }
)

