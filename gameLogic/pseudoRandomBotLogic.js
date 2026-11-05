//https://dota2.fandom.com/wiki/Random_Distribution
// using legacy data
const {
    getCurrentGamePlayer,
    getActionPointer,
    getAction,
    getGameArea,
    emitOpenBubbles,
    setActionPointer,
    emitCloseBubbles,
    setAction,
    setNextPlayer,
    getLobbyData,
    getGamePlayers,
    isDebugGame,
    getAutoplayCounter,
    setAutoplayCounter
} = require("../utils/getGameData");
const {findProbability} = require("./botChance");
const {recordEvent, playerRef} = require("../utils/gameEvents");

function pseudoRandomChoice(lastSuccessfulAttempt) {
    if (Math.random() <= findProbability(lastSuccessfulAttempt)) {
        return [true, 0];
    }

    return [false, lastSuccessfulAttempt + 1];
}

function chooseCell(openedCells, allCells, lastSuccessfulAttempt, knownBubbleValue = null, knownBubbleId = null) {
    const openCellsBubblesImgs = openedCells.map(cell => cell.bubbleImg);

    let filteredCells = [...allCells];

    if (knownBubbleId !== null) {
        filteredCells[knownBubbleId] = -1;
    }

    for (let openImg of openCellsBubblesImgs) {
        const index = filteredCells.indexOf(openImg);
        if (index !== -1) {
            filteredCells[index] = -1;
        }
    }

    const availableCells = filteredCells.map((cell, index) => (cell !== -1) ? index : null).filter(index => index !== null);
    const randomIndex = availableCells[Math.floor(Math.random() * availableCells.length)];

    // Only the second pick (with a known first bubble) rolls the chance; the first pick must not
    // consume or reset the counter, otherwise the shown chance would not match the bot's behaviour.
    if (knownBubbleValue === null) {
        return [randomIndex, lastSuccessfulAttempt];
    }

    const [shouldFindDuplicate, newLastSuccessfulAttempt] = pseudoRandomChoice(lastSuccessfulAttempt);

    if (shouldFindDuplicate) {
        const duplicateIndex = filteredCells.findIndex(cell => cell === knownBubbleValue);

        if (duplicateIndex !== -1) {
            return [duplicateIndex, newLastSuccessfulAttempt];
        }
    }

    return [randomIndex, newLastSuccessfulAttempt];
}

function randomSleep(min, max) {
    return new Promise(resolve => setTimeout(resolve, Math.random() * (max - min) + min));
}

// Debug mode plays every turn without the human-like pauses, so a game finishes in seconds.
const DEBUG_STEP_MS = 40;

// Bots, and in debug mode (see enableDebugMode in gamews) human players too, are played by the
// pseudo-random bot logic. Each player keeps its own counter: bots in LobbyData.bots, humans in
// LobbyData.autoplayAttempts.
// runLocked wraps each state read/write in the game lock; the sleeps between bot clicks
// run outside it so chat, joins and reconnects are not blocked for the whole bot turn.
async function performBotActions(io, redisClient, consul, data, rabbitMQChannel, runLocked = action => action()) {
    const gameKey = `Game:${data.gameUUID}`;
    let debug = await isDebugGame(redisClient, data.gameUUID);
    const pause = (min, max) => debug ? randomSleep(DEBUG_STEP_MS, DEBUG_STEP_MS) : randomSleep(min, max);

    // Returns the player whose turn is played automatically, or null when the game ended or the turn
    // moved on (to a human outside debug mode, or to another player than expected).
    async function currentAutoPlayer(expected) {
        if (!await redisClient.exists(gameKey) || await redisClient.hGet(gameKey, 'GameOver')) return null;
        const player = await getCurrentGamePlayer(redisClient, data.gameUUID);
        if (!player || (player.type !== "Bot" && !debug)) return null;
        if (expected && (player.type !== expected.type || player.id !== expected.id)) return null;
        return player;
    }

    while (true) {
        await pause(1000, 2000);

        const move = await runLocked(async () => {
            debug = await isDebugGame(redisClient, data.gameUUID);
            const player = await currentAutoPlayer();
            if (!player) return null;
            await redisClient.expire(`${gameKey}:BotRunning`, 120);

            const lastAction = await getAction(redisClient, data.gameUUID, await getActionPointer(redisClient, data.gameUUID));
            if (!lastAction || lastAction.openBubbles.length === 100) return null;

            const gameArea = await getGameArea(redisClient, data.gameUUID);
            // CurrentPlayer holds a copy made when the turn started; the counter is kept in LobbyData.
            const counter = getAutoplayCounter(await getLobbyData(redisClient, data.gameUUID), player);
            const [select1, attempt1] = chooseCell(lastAction.openBubbles, gameArea, counter);
            const [select2, attempt2] = chooseCell(lastAction.openBubbles, gameArea, attempt1, gameArea[Number(select1)], select1);
            return {player, gameArea, select1: Number(select1), select2: Number(select2), attempt: attempt2};
        });
        if (!move) return;

        const {player: mover, gameArea, select1, select2} = move;
        const isPair = gameArea[select1] === gameArea[select2];

        await pause(200, 1000);
        await emitOpenBubbles(io, data.gameUUID, select1, Number(gameArea[select1]));
        const openedAt1 = Date.now();

        await pause(200, 1000);
        await emitOpenBubbles(io, data.gameUUID, select2, Number(gameArea[select2]));
        const openedAt2 = Date.now();

        if (!isPair) {
            await pause(1000, 2000);
            await emitCloseBubbles(io, data.gameUUID, select1, select2);
        }

        const keepsTurn = await runLocked(async () => {
            if (!await currentAutoPlayer(mover)) return false;

            const player = playerRef(mover);
            await recordEvent(redisClient, data.gameUUID, {type: 'open', player, bubbleId: select1, bubbleImg: Number(gameArea[select1])}, openedAt1);
            await recordEvent(redisClient, data.gameUUID, {type: 'open', player, bubbleId: select2, bubbleImg: Number(gameArea[select2])}, openedAt2);
            await recordEvent(redisClient, data.gameUUID, {type: isPair ? 'match' : 'miss', player, bubbles: [select1, select2]}, openedAt2);

            await setAutoplayCounter(redisClient, data.gameUUID, mover, move.attempt);
            // Clients show each bot's progress towards a guaranteed find; it changes after every bot move.
            io.to(data.gameUUID).emit('playerList', await getGamePlayers(redisClient, consul, data.gameUUID));

            const pointer = await getActionPointer(redisClient, data.gameUUID);
            const lastAction = await getAction(redisClient, data.gameUUID, pointer);
            const botAction = {
                openBubbles: [...(lastAction ? lastAction.openBubbles : [])],
                requestedBubbles: {}, sender: mover, serverTime: new Date().toISOString()
            };
            if (isPair) {
                const openedBy = {type: mover.type, id: mover.id};
                botAction.openBubbles.push({bubbleId: select1, bubbleImg: gameArea[select1], openedBy}, {
                    bubbleId: select2, bubbleImg: gameArea[select2], openedBy
                });
            }

            await setAction(redisClient, data.gameUUID, pointer + 1, botAction);
            await setActionPointer(redisClient, data.gameUUID, pointer + 1);
            io.to(data.gameUUID).emit('gameAction', botAction);

            if (isPair) return botAction.openBubbles.length < 100;

            const nextPlayer = await setNextPlayer(redisClient, consul, data.gameUUID,
                mover.type === "Bot" ? mover : {UserId: mover.id});
            io.to(data.gameUUID).emit('currentPlayer', nextPlayer);
            return nextPlayer.type === "Bot" || await isDebugGame(redisClient, data.gameUUID);
        });
        if (!keepsTurn) return;
    }
}

module.exports = {
    chooseCell, randomSleep, performBotActions
}