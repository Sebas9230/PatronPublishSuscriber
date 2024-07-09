const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: [''],
    ssl: true,
    sasl: {
        mechanism: 'plain',
        username: '',
        password: ''
    },
    connectionTimeout: 10000 // 10 segundos
});

const consumer = kafka.consumer({ groupId: 'science-group' });

const run = async () => {
    // Consumidor
    await consumer.connect();
    await consumer.subscribe({ topic: 'science', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Mensaje recibido: ${message.value.toString()}`);
        },
    });
};

run().catch(console.error);

const port = 3001;
app.listen(port, () => console.log(`Servidor corriendo en puerto ${port}`));
