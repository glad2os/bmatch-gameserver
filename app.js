const express = require('express');
const redis = require('redis');
const cors = require("cors");
const Consul = require('consul');
const fs = require('fs');
const path = require('path');
const {consulHost, consulPort, getRedisConfig, getRabbitMQConfig} = require("./config/config");
const {startLobbyServer} = require('./servers/lobbyServer');
const {startGameServer} = require('./servers/gameServer');
const {connect} = require("amqplib");

const app = express();

const useTLS = process.env.USE_TLS === 'true';

let options = {};

if (useTLS) {
    const tlsCertPath = path.join(__dirname, 'tls', process.env.TLS_CERT_FILE || 'dev-cert.crt');
    const tlsKeyPath = path.join(__dirname, 'tls', process.env.TLS_KEY_FILE || 'dev-key.key');
    options.key = fs.readFileSync(tlsKeyPath);
    options.cert = fs.readFileSync(tlsCertPath);
}

const consul = new Consul({
    host: consulHost, port: consulPort
});

let redisClient;

const initializeServices = async () => {
    const redisHost = await getRedisConfig(consul);
    const rabbitMQConfig = await getRabbitMQConfig(consul);

    redisClient = redis.createClient({
        url: `redis://${redisHost.host}:${redisHost.port}/`,
    });

    redisClient.on('error', err => console.log('Redis Client Error', err));
    await redisClient.connect();

    const rabbitMQConnection  = await connect(`amqp://${rabbitMQConfig.user}:${rabbitMQConfig.pass}@${rabbitMQConfig.host}:${rabbitMQConfig.port}`);
    const rabbitMQChannel  = await rabbitMQConnection.createChannel();
    console.log(`Connected to RabbitMQ at ${rabbitMQConfig.host}:${rabbitMQConfig.port}`)

    startLobbyServer(app, options, consul, redisClient);
    await startGameServer(app, options, consul, redisClient, rabbitMQChannel);
}

initializeServices().catch(err => {
    console.error("Failed to initialize services:", err);
    process.exit(1);
});

app.use(express.json());
app.use(express.urlencoded({extended: false}));
app.use(cors({
    origin: '*'
}));

app.get('/health', (req, res) => {
    res.send("Server is running");
});

