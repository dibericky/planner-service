# Planner Service

This is a component of the PopCorn Planner application.

This service aims to handle the creation of the watching plan of the tv series.

When a "create-plan" message is received, if the required tv serie is not already present in the DB, a message of "tvserie retrieve" is sent.

When a "tvserie saved" is received, a plan is created on MongoDB and a related message is sent.

## Local Testing

Install the dependencies

```
npm ci
```

Run the docker images of MongoDB and RabbitMQ

```
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.9
docker run -it --rm --name mongo4.4 -p 27017:27017 mongo:4.4
```

Run the tests

```
npm t
```