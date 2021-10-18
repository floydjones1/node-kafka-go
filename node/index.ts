import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:29092"],
});

enum TOPICS {
  GO_FACTORIAL_TOPIC = "GO_FACTORIAL_TOPIC",
  GO_FACTORIAL_UPDATE = "GO_FACTORIAL_UPDATE",
  NODE_PRIME_TOPIC = "NODE_PRIME_TOPIC",
  NODE_PRIME_UPDATE = "NODE_PRIME_UPDATE",
}

const someLongRunningTask = async (): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, 2000));
};

async function main() {
  const consumer = kafka.consumer({ groupId: "test-group" });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.NODE_PRIME_TOPIC });
  const dispath = await produce();
  await consumer.run({
    eachBatchAutoResolve: true,
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: message?.value?.toString(),
      });
      console.log(message.key.toJSON());
      someLongRunningTask().then(dispath);
    },
    // eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
    //   // console.log(batch.messages);

    //   const offset = batch.messages[batch.messages.length - 1].offset;
    //   batch.messages.forEach((m) => console.log(m.value.toString()));
    //   console.log(batch.messages.length);
    //   console.log(offset);
    //   resolveOffset(offset);
    //   await heartbeat();
    // },
  });
}

const produce = async () => {
  const producer = kafka.producer();
  await producer.connect();
  return async () => {
    const randomWaitTime = Math.floor(Math.random() * 10000);

    await new Promise((resolve) => setTimeout(resolve, randomWaitTime));

    await producer.send({
      topic: TOPICS.NODE_PRIME_UPDATE,
      messages: [
        {
          key: Math.random().toString(),
          value: "Success",
        },
      ],
    });
  };
};

main();
