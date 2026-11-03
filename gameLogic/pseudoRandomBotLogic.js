//https://dota2.fandom.com/wiki/Random_Distribution
// using legacy data
const {
    getCurrentGamePlayer,
    getActionPointer,
    getAction,
    getGameArea,
    emitOpenBubbles,
    setBotLastSuccessfulAttempt,
    setActionPointer,
    emitCloseBubbles,
    setAction,
    setNextPlayer
} = require("../utils/getGameData");

function pseudoRandomChoice(lastSuccessfulAttempt) {
    const C = 0.05;
    const probability = C * (lastSuccessfulAttempt + 1);

    const randomValue = Math.random();

    if (randomValue <= probability) {
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

    const [shouldFindDuplicate, newLastSuccessfulAttempt] = pseudoRandomChoice(lastSuccessfulAttempt);

    if (shouldFindDuplicate && knownBubbleValue !== null) {
        const duplicateIndex = filteredCells.findIndex(cell => cell === knownBubbleValue);

        if (duplicateIndex !== -1) {
            return [duplicateIndex, newLastSuccessfulAttempt];
        }
    }

    let randomIndex = availableCells[Math.floor(Math.random() * availableCells.length)];
    return [randomIndex, newLastSuccessfulAttempt];
}

function randomSleep(min, max) {
    return new Promise(resolve => setTimeout(resolve, Math.random() * (max - min) + min));
}

// runLocked wraps each state read/write in the game lock; the sleeps between bot clicks
// run outside it so chat, joins and reconnects are not blocked for the whole bot turn.
async function performBotActions(io, redisClient, consul, data, rabbitMQChannel, runLocked = action => action()) {
    const gameKey = `Game:${data.gameUUID}`;

    // Returns the bot that still owns the turn, or null when the game ended or the turn moved on.
    async function currentBot(expectedId) {
        if (!await redisClient.exists(gameKey) || await redisClient.hGet(gameKey, 'GameOver')) return null;
        const player = await getCurrentGamePlayer(redisClient, data.gameUUID);
        if (!player || player.type !== "Bot" || (expectedId !== undefined && player.id !== expectedId)) return null;
        return player;
    }

    while (true) {
        await randomSleep(1000, 2000);

        const move = await runLocked(async () => {
            const bot = await currentBot();
            if (!bot) return null;
            await redisClient.expire(`${gameKey}:BotRunning`, 120);

            const lastAction = await getAction(redisClient, data.gameUUID, await getActionPointer(redisClient, data.gameUUID));
            if (!lastAction || lastAction.openBubbles.length === 100) return null;

            const gameArea = await getGameArea(redisClient, data.gameUUID);
            const [select1, attempt1] = chooseCell(lastAction.openBubbles, gameArea, bot.lastSuccessfulAttempt);
            const [select2, attempt2] = chooseCell(lastAction.openBubbles, gameArea, attempt1, gameArea[Number(select1)], select1);
            return {bot, gameArea, select1: Number(select1), select2: Number(select2), attempt: attempt2};
        });
        if (!move) return;

        const {bot, gameArea, select1, select2} = move;
        const isPair = gameArea[select1] === gameArea[select2];

        await randomSleep(200, 1000);
        await emitOpenBubbles(io, data.gameUUID, select1, Number(gameArea[select1]));

        await randomSleep(200, 1000);
        await emitOpenBubbles(io, data.gameUUID, select2, Number(gameArea[select2]));

        if (!isPair) {
            await randomSleep(1000, 2000);
            await emitCloseBubbles(io, data.gameUUID, select1, select2);
        }

        const keepsTurn = await runLocked(async () => {
            if (!await currentBot(bot.id)) return false;

            await setBotLastSuccessfulAttempt(redisClient, data.gameUUID, bot.id, move.attempt);

            const pointer = await getActionPointer(redisClient, data.gameUUID);
            const lastAction = await getAction(redisClient, data.gameUUID, pointer);
            const botAction = {
                openBubbles: [...(lastAction ? lastAction.openBubbles : [])],
                requestedBubbles: {}, sender: bot, serverTime: new Date().toISOString()
            };
            if (isPair) {
                botAction.openBubbles.push({bubbleId: select1, bubbleImg: gameArea[select1]}, {
                    bubbleId: select2, bubbleImg: gameArea[select2]
                });
            }

            await setAction(redisClient, data.gameUUID, pointer + 1, botAction);
            await setActionPointer(redisClient, data.gameUUID, pointer + 1);
            io.to(data.gameUUID).emit('gameAction', botAction);

            if (isPair) return botAction.openBubbles.length < 100;

            const nextPlayer = await setNextPlayer(redisClient, consul, data.gameUUID, bot);
            io.to(data.gameUUID).emit('currentPlayer', nextPlayer);
            return nextPlayer.type === "Bot";
        });
        if (!keepsTurn) return;
    }
}

module.exports = {
    chooseCell, randomSleep, performBotActions
}