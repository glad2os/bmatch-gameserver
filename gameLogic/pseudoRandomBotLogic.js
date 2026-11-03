//https://dota2.fandom.com/wiki/Random_Distribution
// using legacy data
const {
    getCurrentGamePlayer,
    getActionPointer,
    getAction,
    getGameArea,
    emitOpenBubbles,
    setBotLastSuccessfulAttempt,
    setPaused,
    setActionPointer,
    emitCloseBubbles,
    setAction,
    setNextPlayer, cancelTimer, setTimer
} = require("../utils/getGameData");
const {endGame} = require("../utils/endGameHandler");

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

async function performBotActions(io, redisClient, consul, data, rabbitMQChannel) {
    let nextPlayer = await getCurrentGamePlayer(redisClient, data.gameUUID);

    while (nextPlayer.type === "Bot") {
        await redisClient.expire(`Game:${data.gameUUID}:BotRunning`, 120);
        let botAp = await getActionPointer(redisClient, data.gameUUID);
        let chCellBotAction = await getAction(redisClient, data.gameUUID, botAp);

        if (chCellBotAction.openBubbles.length === 100) {
            console.log(chCellBotAction.openBubbles.length);
            return;
        }

        let nextTry = false;

        await randomSleep(1000, 2000);

        let gameArea = await getGameArea(redisClient, data.gameUUID);

        const [botSelect1, newLastSuccessfulAttempt1] = chooseCell(chCellBotAction.openBubbles, gameArea, nextPlayer.lastSuccessfulAttempt);
        const [botSelect2, newLastSuccessfulAttempt2] = chooseCell(chCellBotAction.openBubbles, gameArea, newLastSuccessfulAttempt1, gameArea[Number(botSelect1)], botSelect1);

        await randomSleep(200, 1000);
        await emitOpenBubbles(io, data.gameUUID, Number(botSelect1), Number(gameArea[Number(botSelect1)]));

        await randomSleep(200, 1000);
        await emitOpenBubbles(io, data.gameUUID, Number(botSelect2), Number(gameArea[Number(botSelect2)]));

        await setBotLastSuccessfulAttempt(redisClient, data.gameUUID, nextPlayer.id, newLastSuccessfulAttempt2);

        let botActionPointer = await getActionPointer(redisClient, data.gameUUID);

        let botLastAction = await getAction(redisClient, data.gameUUID, botActionPointer);

        let previousOpenBubbles = botLastAction ? botLastAction.openBubbles : [];

        let incrementedBotActionPointer = ++botActionPointer;
        await setActionPointer(redisClient, data.gameUUID, incrementedBotActionPointer);

        let botAction = {
            openBubbles: [...previousOpenBubbles], requestedBubbles: [{
                bubbleId: Number(botSelect1), bubbleImg: Number(gameArea[Number(botSelect1)])
            }, {
                bubbleId: Number(botSelect2), bubbleImg: Number(gameArea[Number(botSelect2)])
            }], sender: nextPlayer, serverTime: new Date().toISOString()
        }

        if (gameArea[botSelect1] !== gameArea[botSelect2]) {
            await randomSleep(1000, 2000);

            await emitCloseBubbles(io, data.gameUUID, Number(botSelect1), Number(botSelect2))
        } else {
            botAction.openBubbles.push({bubbleId: botSelect1, bubbleImg: gameArea[botSelect1]}, {
                bubbleId: botSelect2, bubbleImg: gameArea[botSelect2]
            });

            nextTry = true;
        }

        botAction.requestedBubbles = {};
        await setAction(redisClient, data.gameUUID, incrementedBotActionPointer, botAction);
        io.to(data.gameUUID).emit('gameAction', botAction);

        if (!nextTry) {
            nextPlayer = await setNextPlayer(redisClient, consul, data.gameUUID, nextPlayer);
            io.to(data.gameUUID).emit('currentPlayer', nextPlayer);
        }
    }
}

module.exports = {
    chooseCell, randomSleep, performBotActions
}