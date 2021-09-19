'use strict'

require('dotenv').config()

const amqp = require('amqplib')
var axios = require("axios").default;
const { MongoClient } = require('mongodb');
const pinoLogger = require('pino')({ level: 'debug' })

const TVSERIES_COLLECTION = 'tvseries'

const QUEUE_RETRIEVE = 'popcorn-planner.tvserie-retrieve'
const QUEUE_CREATE_PLAN = 'popcorn-planner.create-plan'

async function connectMongo(logger, connectionString) {
    logger.info('connecting to MongoDB')
    const client = new MongoClient(connectionString, { useNewUrlParser: true, useUnifiedTopology: true });
    await client.connect()
    logger.info('connected to MongoDB')
    return client
}

async function getTvSerieData(logger, mongoDb, title) {
    const tvSerie = await mongoDb.collection(TVSERIES_COLLECTION).findOne({ title: title })
    logger.debug({ tvSerie, title, collection: TVSERIES_COLLECTION }, 'document found')
}

function sendMessageToRetrieveTvSerie(logger, channel, title) {
    logger.info({ title }, 'sending message to retrieve tv serie')
    const msg = JSON.stringify({ name: title })
    channel.assertQueue(QUEUE_RETRIEVE, {
        durable: true
    })
    channel.sendToQueue(QUEUE_RETRIEVE, Buffer.from(msg), {
        persistent: true
    })
    logger.debug({ title, queue: QUEUE_RETRIEVE }, 'sent message to retrieve tv serie')
}

async function handleConsumeCreatePlan(logger, msg, mongoDb, channel) {
    const { title } = msg
    const tvSerie = await getTvSerieData(logger, mongoDb, title)
    if (!tvSerie) {
        sendMessageToRetrieveTvSerie(logger, channel, title)
        return
    }
    throw new Error('unimplemented')
}

async function run(logger, { RABBITMQ_CONN_STRING, MONGODB_CONN_STRING }) {
    const mongoDbClient = await connectMongo(logger, MONGODB_CONN_STRING)
    const mongoDb = mongoDbClient.db()

    const connection = await amqp.connect(RABBITMQ_CONN_STRING)
    const channel = await connection.createChannel()

    channel.assertQueue(QUEUE_CREATE_PLAN, {
        durable: true
    });
    channel.prefetch(1)
    logger.debug({ queue: QUEUE_CREATE_PLAN }, "waiting for messages in queue")
    channel.consume(QUEUE_CREATE_PLAN, (msg) => {
        logger.debug({ msg: msg.content.toString() }, 'received message')
        handleConsumeCreatePlan(logger, JSON.parse(msg.content.toString()), mongoDb, channel)
            .then(() => {
                logger.info({ queue: QUEUE_CREATE_PLAN }, 'Sending ack of create-plan')
                channel.ack(msg)
            })
    }, {
        noAck: false
    })



    return async () => {
        await mongoDbClient.close()
        await connection.close()
    }
}

module.exports = run

if (require.main === module) {
    run(pinoLogger, process.env)
}