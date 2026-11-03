function validateMove(rawBubbleId, lastAction, actionPoints) {
    if (rawBubbleId === undefined || rawBubbleId === null ||
        !['number', 'string'].includes(typeof rawBubbleId) || String(rawBubbleId).trim() === '') return false;
    const bubbleId = Number(rawBubbleId);
    if (!Number.isInteger(bubbleId) || bubbleId < 0 || bubbleId >= 100) return false;
    if (!lastAction || ![1, 2].includes(actionPoints)) return false;
    if (lastAction.openBubbles.some(b => Number(b.bubbleId) === bubbleId)) return false;
    if (actionPoints === 1 && bubbleId === Number(lastAction.requestedBubbles.bubbleId)) return false;
    return true;
}
module.exports = {validateMove};
