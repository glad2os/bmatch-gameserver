// Pseudo-random distribution for bots: the chance to deliberately find a pair grows with every turn
// without such a find and reaches 100% after GUARANTEE_TURNS turns. A steep curve keeps early finds
// rare, so the progress shown to players (k / GUARANTEE_TURNS) predicts the find: half of the finds
// happen at >= 83% progress, 90% at >= 67%; on average a find takes 5 turns (the previous linear
// 5%-per-turn curve averaged 5.3 turns but mostly fired at 10-45%).
const GUARANTEE_TURNS = 6;
const CURVE_POWER = 4;

// lastSuccessfulAttempt: bot turns since the last deliberate find.
function turnsTowardsFind(lastSuccessfulAttempt) {
    return Math.min(GUARANTEE_TURNS, (lastSuccessfulAttempt || 0) + 1);
}

// Probability (0..1) of a deliberate find on the bot's next turn.
function findProbability(lastSuccessfulAttempt) {
    return (turnsTowardsFind(lastSuccessfulAttempt) / GUARANTEE_TURNS) ** CURVE_POWER;
}

// Progress (0..100) towards the guaranteed find on the bot's next turn, shown to players.
function findProgress(lastSuccessfulAttempt) {
    return Math.round(turnsTowardsFind(lastSuccessfulAttempt) / GUARANTEE_TURNS * 100);
}

module.exports = {findProbability, findProgress, GUARANTEE_TURNS};
