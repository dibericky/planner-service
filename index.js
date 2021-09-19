'use strict'

require('dotenv').config()

const amqp = require('amqplib')
var axios = require("axios").default;
const { MongoClient } = require('mongodb');
const pinoLogger = require('pino')({ level: 'debug' })

const TVSERIES_COLLECTION = 'tvseries'
const PLANS_COLLECTION = 'plans'

const QUEUE_RETRIEVE = 'popcorn-planner.tvserie-retrieve'
const QUEUE_SAVED = 'popcorn-planner.tvserie-saved'
const QUEUE_CREATE_PLAN = 'popcorn-planner.create-plan'
const QUEUE_CREATED_PLAN = 'popcorn-planner.plan-created'

const PLAN_STATE = {
    CREATED: 'created',
    CREATING: 'creating'
}

async function connectMongo(logger, connectionString) {
    logger.info('connecting to MongoDB')
    const client = new MongoClient(connectionString, { useNewUrlParser: true, useUnifiedTopology: true });
    await client.connect()
    logger.info('connected to MongoDB')
    return client
}

async function initializeCollection (logger, mongoDb) {
    const createIndex = async () => {
        logger.info('Creating index')
        await mongoDb.collection(PLANS_COLLECTION).createIndex({
            "title": 1
        },
        {
            unique: true
        })
    }
    let indexes = []
    try {
        indexes = await mongoDb.collection(PLANS_COLLECTION).indexes()
    } catch (err) {
        logger.info('Collection %s does not exist, it will be created', PLANS_COLLECTION)
        await createIndex()
        return
    }
    if (!indexes.find(index => index.key.title)) {
        await createIndex()
    }
}

async function getTvSerieData(logger, mongoDb, title) {
    const tvSerie = await mongoDb.collection(TVSERIES_COLLECTION).findOne({ title: title })
    logger.debug({ tvSerie, title, collection: TVSERIES_COLLECTION }, 'document found')
    return tvSerie
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
    const { title, episodesPerDay } = msg
    const tvSerie = await getTvSerieData(logger, mongoDb, title)
    if (!tvSerie) {
        sendMessageToRetrieveTvSerie(logger, channel, title)
        await savePlan(logger, mongoDb, title, {episodesPerDay, planState: PLAN_STATE.CREATING})
        return
    }
    const {numberOfEpisodes} = tvSerie
    await savePlan(logger, mongoDb, title, {
        episodesPerDay,
        totalDays: getTotalDaysPlan(numberOfEpisodes, episodesPerDay),
        planState: PLAN_STATE.CREATED
    })
    sendMessagePlanCreated(logger, title, channel)
}

async function savePlan(logger, mongoDb, title, values) {
    logger.debug({collection: PLANS_COLLECTION, title, ...values}, 'saving plan')
    await mongoDb.collection(PLANS_COLLECTION).updateOne({
        title
    }, {
        $set: values
    }, {upsert: true})
}

function getTotalDaysPlan (numberOfEpisodes, episodesPerDay) {
    return parseInt(numberOfEpisodes / episodesPerDay, 10) + (numberOfEpisodes%episodesPerDay > 0 ? 1 : 0)
}

async function createPlan (logger, mongoDb, title, numberOfEpisodes) {
    logger.debug({title}, 'looking for plan in creating state for tv serie')
    const plan = await mongoDb.collection(PLANS_COLLECTION).findOne({
        title,
        planState: PLAN_STATE.CREATING
    })
    if (!plan) {
        throw new Error('Expected to find a plan in creating state for '+title)
    }
    const {episodesPerDay} = plan
    return {
        totalDays: getTotalDaysPlan(numberOfEpisodes, episodesPerDay),
        title
    }
}

async function handleConsumeTvSerieSaved(logger, msg, mongoDb) {
    const { title } = msg
    const tvSerie = await getTvSerieData(logger, mongoDb, title)
    if (!tvSerie) {
        throw new Error('Unexpected tv serie not found '+title)
    }
    const plan = await createPlan(logger, mongoDb, title, tvSerie.numberOfEpisodes)
    await savePlan(logger, mongoDb, title, {planState: PLAN_STATE.CREATED, totalDays: plan.totalDays})
    return {
        title
    }
}

function sendMessagePlanCreated (logger, title, channel) {
    logger.debug({queue: QUEUE_CREATED_PLAN, title}, 'sending message that plan has been created')
    channel.assertQueue(QUEUE_CREATED_PLAN, {
        durable: true
    })
    channel.sendToQueue(QUEUE_CREATED_PLAN, Buffer.from(JSON.stringify({title})), {
        persistent: true
    })
}

async function run(logger, { RABBITMQ_CONN_STRING, MONGODB_CONN_STRING }) {
    const mongoDbClient = await connectMongo(logger, MONGODB_CONN_STRING)
    const mongoDb = mongoDbClient.db()
    await initializeCollection(logger, mongoDb)

    const connection = await amqp.connect(RABBITMQ_CONN_STRING)
    const channel = await connection.createChannel()

    channel.assertQueue(QUEUE_CREATE_PLAN, {
        durable: true
    });
    channel.prefetch(1)
    logger.debug({ queue: QUEUE_CREATE_PLAN }, "waiting for messages in queue")
    channel.consume(QUEUE_CREATE_PLAN, (msg) => {
        logger.debug({ queue: QUEUE_CREATE_PLAN, msg: msg.content.toString() }, 'received message')
        handleConsumeCreatePlan(logger, JSON.parse(msg.content.toString()), mongoDb, channel)
            .then(() => {
                logger.info({ queue: QUEUE_CREATE_PLAN }, 'Sending ack of create-plan')
                channel.ack(msg)
            })
    }, {
        noAck: false
    })

    channel.assertQueue(QUEUE_SAVED, {
        durable: true
    });
    logger.debug({ queue: QUEUE_SAVED }, "waiting for messages in queue")
    channel.consume(QUEUE_SAVED, (msg) => {
        logger.debug({ queue: QUEUE_SAVED, msg: msg.content.toString() }, 'received message')
        handleConsumeTvSerieSaved(logger, JSON.parse(msg.content.toString()), mongoDb, channel)
            .then(({title}) => {
                sendMessagePlanCreated(logger, title, channel)
                logger.info({ queue: QUEUE_SAVED }, 'Sending ack of tvserie-saved')
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