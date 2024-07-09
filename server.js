const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: [''],
    ssl: {
        rejectUnauthorized: false,
    },
    sasl: {
        mechanism: 'plain',
        username: '',
        password: ''
    },
    connectionTimeout: 10000 // 10 segundos
});

const consumer = kafka.consumer({ groupId: 'sports-group' });

const producer = kafka.producer();
app.post('/publish', async (req, res) => {
    const message = req.body;
    await producer.send({
        topic: 'science',
        messages: [
            { value: JSON.stringify(message) },
        ],
    });
    res.status(200).send('Mensaje publicado con éxito');
});

app.post('/publish_sports', async (req, res) => {
    const message = req.body;
    await producer.send({
        topic: 'sports',
        messages: [
            { value: JSON.stringify(message) },
        ],
    });
    res.status(200).send('Mensaje publicado con éxito');
});

const run = async () => {
    // Consumidor
    await consumer.connect();
    await consumer.subscribe({ topic: 'sports', fromBeginning: true });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Mensaje recibido: ${message.value.toString()}`);
        },
    });

    // Productor
    await producer.connect();
};

run().catch(console.error);

const port = 3000;
app.listen(port, () => console.log(`Servidor corriendo en puerto ${port}`));
