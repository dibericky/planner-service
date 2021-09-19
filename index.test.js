'use strict'

const tap = require('tap')
const {MongoClient} = require('mongodb')
const amqp = require('amqplib')
const logger = require('pino')({level: 'debug'})
const {omit, all} = require('ramda')
const sinon = require('sinon')

const main = require('./index')

const envs = {
    RABBITMQ_CONN_STRING: 'amqp://localhost',
    MONGODB_CONN_STRING: 'mongodb://127.0.0.1:27017/db-test'
}

const createPlanQueue = 'popcorn-planner.create-plan'
const retrieveQueue = 'popcorn-planner.tvserie-retrieve'

tap.test('main', t => {
    t.test('on "create-plan" message received', t => {
        let mongoDbClient, rabbitMqConnection, channel

        t.beforeEach(async () => {
            mongoDbClient = new MongoClient(envs.MONGODB_CONN_STRING, { useNewUrlParser: true, useUnifiedTopology: true });
            await mongoDbClient.connect()
            await mongoDbClient.db().dropDatabase()

            rabbitMqConnection = await amqp.connect(envs.RABBITMQ_CONN_STRING)
            channel = await rabbitMqConnection.createChannel()
            await channel.deleteQueue(createPlanQueue)
            await channel.deleteQueue(retrieveQueue)
        })

        t.afterEach(async () => {
            await channel.deleteQueue(createPlanQueue)
            await channel.deleteQueue(retrieveQueue)
            
            rabbitMqConnection.close()

            await mongoDbClient.db().dropDatabase()
            await mongoDbClient.close()
        })
        t.test('if title does not exist in collection, send "retrieve" title', async t => {
            channel.assertQueue(retrieveQueue, {
                durable: true
            });
            const savedCallbackMock = sinon.spy()
            channel.consume(retrieveQueue, savedCallbackMock, {noAck: true})
            
            const allTvSeries = await mongoDbClient.db().collection('tvseries').find({}).toArray()
            t.strictSame(allTvSeries, [])

            const close = await main(logger, envs)
            await sendTestMessage(channel)
    
            await wait(1000)

            t.ok(savedCallbackMock.calledOnce)
            const {args} = savedCallbackMock.getCall(0)
            t.strictSame(args.length, 1)
            t.strictSame(JSON.parse(args[0].content.toString()), {name: 'Supernatural'})

            await close()
        
            t.end()
        })
        t.end()
    })
    t.end()
})




async function wait (time) { 
    return new Promise(resolve => setTimeout(resolve, time))
}


async function sendTestMessage (channel) {
    const msg = JSON.stringify({title: 'Supernatural'})
    channel.assertQueue(createPlanQueue, {
        durable: true
    })
    channel.sendToQueue(createPlanQueue, Buffer.from(msg), {
        persistent: true
    })
}