const https = require('https');
const { Server } = require("socket.io");
const { joinLobby, disconnectLobby, generateGame } = require("./../sockets/lobbyws");
const http = require("http");
const useTLS = process.env.USE_TLS === 'true';

function startLobbyServer(app, options, consul, redisClient) {
    const server = useTLS
        ? https.createServer(options, app)
        : http.createServer(app);

    const io = new Server(server, {
        path: "/socket.io",
        cors: {
            origin: '*',
            methods: ["GET", "POST"]
        }
    });

    io.on('connection', socket => {
        joinLobby(io, socket, consul, redisClient);
        disconnectLobby(io, socket, redisClient);
        generateGame(io, socket, redisClient, consul)
    });

    server.listen(8004, () => {
        console.log("Lobby Socket.IO server is running on 8004");
    });
}

module.exports = { startLobbyServer };
