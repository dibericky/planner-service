'use strict'

const tap = require('tap')
const {MongoClient} = require('mongodb')
const amqp = require('amqplib')
const logger = require('pino')({level: 'trace'})
const {omit} = require('ramda')
const sinon = require('sinon')

const main = require('./index')

const envs = {
    RABBITMQ_CONN_STRING: 'amqp://localhost',
    MONGODB_CONN_STRING: 'mongodb://127.0.0.1:27017/db-test'
}

const createPlanQueue = 'popcorn-planner.create-plan'
const retrieveQueue = 'popcorn-planner.tvserie-retrieve'
const savedQueue = 'popcorn-planner.tvserie-saved'
const createdPlanQueue = 'popcorn-planner.plan-created'

tap.test('main', t => {
    let mongoDbClient

    t.before(async () => {
        mongoDbClient = new MongoClient(envs.MONGODB_CONN_STRING, { useNewUrlParser: true, useUnifiedTopology: true });
        await mongoDbClient.connect()
    })

    t.teardown(async () => {
        await mongoDbClient.close()            
    })

    t.test('on "create-plan" message received', t => {
        let rabbitMqConnection, channel

        t.beforeEach(async () => {
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
        })
        t.test('if title does not exist in collection, send "retrieve" title', async t => {
            channel.assertQueue(retrieveQueue, {
                durable: true
            });
            const callbackMock = sinon.spy()
            channel.consume(retrieveQueue, callbackMock, {noAck: true})
            
            const allTvSeries = await mongoDbClient.db().collection('tvseries').find({}).toArray()
            t.strictSame(allTvSeries, [])

            const close = await main(logger, envs)
            await sendCreatePlanTestMessage(channel)
    
            await wait(1000)

            t.ok(callbackMock.calledOnce)
            const {args} = callbackMock.getCall(0)
            t.strictSame(args.length, 1)
            t.strictSame(JSON.parse(args[0].content.toString()), {name: 'Supernatural'})

            const plans = await mongoDbClient.db().collection('plans').find({}).toArray()
            t.strictSame(plans.length, 1)
            t.strictSame(omit(['_id'], plans[0]), {
                title: 'Supernatural',
                planState: 'creating',
                episodesPerDay: 3
            })

            await close()
        
            t.end()
        })

        t.test('if title exists in collection, generate plan for saved tv-serie', async t => {
            const plansBefore = await mongoDbClient.db().collection('plans').find({}).toArray()
            t.strictSame(plansBefore.length, 0)

            channel.assertQueue(retrieveQueue, {
                durable: true
            });
            const retrieveCallbackMock = sinon.spy()
            channel.consume(retrieveQueue, retrieveCallbackMock, {noAck: true})
            
            channel.assertQueue(createdPlanQueue, {
                durable: true
            });
            const createPlanCallbackMock = sinon.spy()
            channel.consume(createdPlanQueue, createPlanCallbackMock, {noAck: true})
            
            await mongoDbClient.db().collection('tvseries').insertOne({
                serieId: 'the-serie-id',
                numberOfEpisodes: 300,
                title: 'Supernatural',
                createdAt: new Date(),
                updatedAt: new Date()
            })

            const close = await main(logger, envs)
            await sendCreatePlanTestMessage(channel)
    
            await wait(1000)

            t.equal(retrieveCallbackMock.callCount, 0)

            t.equal(createPlanCallbackMock.callCount, 1)
            const {args} = createPlanCallbackMock.getCall(0)
            t.strictSame(args.length, 1)
            t.strictSame(JSON.parse(args[0].content.toString()), {title: 'Supernatural'})

            const plans = await mongoDbClient.db().collection('plans').find({}).toArray()
            t.strictSame(plans.length, 1)
            t.strictSame(omit(['_id'], plans[0]), {
                title: 'Supernatural',
                planState: 'created',
                episodesPerDay: 3,
                totalDays: 100
            })

            await close()
        
            t.end()
        })
        t.end()
    })

    t.test('on "tvserie-saved" message received', t => {
        let rabbitMqConnection, channel

        t.beforeEach(async () => {
            await mongoDbClient.db().dropDatabase()

            rabbitMqConnection = await amqp.connect(envs.RABBITMQ_CONN_STRING)
            channel = await rabbitMqConnection.createChannel()
            await channel.deleteQueue(savedQueue)
            await channel.deleteQueue(createdPlanQueue)
        })

        t.afterEach(async () => {
            await channel.deleteQueue(savedQueue)
            await channel.deleteQueue(createdPlanQueue)
            
            rabbitMqConnection.close()
        })
        t.test('generate plan for saved tv-serie', async t => {
            channel.assertQueue(createdPlanQueue, {
                durable: true
            });
            const callbackMock = sinon.spy()
            channel.consume(createdPlanQueue, callbackMock, {noAck: true})
            
            await mongoDbClient.db().collection('tvseries').insertOne({
                serieId: 'the-serie-id',
                numberOfEpisodes: 300,
                title: 'Supernatural',
                createdAt: new Date(),
                updatedAt: new Date()
            })

            await mongoDbClient.db().collection('plans').insertOne({
                title: 'Supernatural',
                planState: 'creating',
                episodesPerDay: 3
            })

            const close = await main(logger, envs)
            await sendSavedTvSerieTestMessage(channel)
    
            await wait(1000)

            t.ok(callbackMock.calledOnce)
            const {args} = callbackMock.getCall(0)
            t.strictSame(args.length, 1)
            t.strictSame(JSON.parse(args[0].content.toString()), {title: 'Supernatural'})

            const plans = await mongoDbClient.db().collection('plans').find({}).toArray()
            t.strictSame(plans.length, 1)
            t.strictSame(omit(['_id'], plans[0]), {
                title: 'Supernatural',
                planState: 'created',
                episodesPerDay: 3,
                totalDays: 100
            })

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


async function sendCreatePlanTestMessage (channel) {
    const msg = JSON.stringify({title: 'Supernatural', episodesPerDay: 3})
    channel.assertQueue(createPlanQueue, {
        durable: true
    })
    channel.sendToQueue(createPlanQueue, Buffer.from(msg), {
        persistent: true
    })
}

async function sendSavedTvSerieTestMessage (channel) {
    const msg = JSON.stringify({title: 'Supernatural'})
    channel.assertQueue(savedQueue, {
        durable: true
    })
    channel.sendToQueue(savedQueue, Buffer.from(msg), {
        persistent: true
    })
}