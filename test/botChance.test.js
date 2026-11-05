const {test} = require('node:test');
const assert = require('node:assert/strict');
const {findProbability, findProgress, GUARANTEE_TURNS} = require('../gameLogic/botChance');
const {chooseCell} = require('../gameLogic/pseudoRandomBotLogic');

test('progress fills in GUARANTEE_TURNS turns and the find is then guaranteed', () => {
    assert.equal(findProgress(0), 17);
    assert.equal(findProgress(undefined), 17);
    assert.equal(findProgress(2), 50);
    assert.equal(findProgress(GUARANTEE_TURNS - 1), 100);
    assert.equal(findProgress(40), 100);
    assert.equal(findProbability(GUARANTEE_TURNS - 1), 1);
    assert.equal(findProbability(40), 1);
});

test('early finds are rare and the chance rises steeply towards the end', () => {
    const chances = Array.from({length: GUARANTEE_TURNS}, (_, n) => findProbability(n));
    assert.ok(chances[0] < 0.01);
    assert.ok(chances[2] < 0.1);
    chances.slice(1).forEach((chance, i) => assert.ok(chance > chances[i]));
    // Probability that a deliberate find has happened by the time progress reaches 50%.
    const findByHalf = 1 - chances.slice(0, 3).reduce((notYet, chance) => notYet * (1 - chance), 1);
    assert.ok(findByHalf < 0.1);
});

test('first pick does not consume the chance; second pick finds the pair when the roll succeeds', t => {
    const area = Array.from({length: 100}, (_, i) => Math.floor(i / 2) + 1);
    const random = t.mock.method(Math, 'random', () => 0);
    const [first, counterAfterFirst] = chooseCell([], area, 7);
    assert.equal(counterAfterFirst, 7);
    const [second, counterAfterSecond] = chooseCell([], area, counterAfterFirst, area[first], first);
    assert.notEqual(second, first);
    assert.equal(area[second], area[first]);
    assert.equal(counterAfterSecond, 0);
    random.mock.restore();
});

test('failed roll increments the counter', t => {
    const area = Array.from({length: 100}, (_, i) => Math.floor(i / 2) + 1);
    t.mock.method(Math, 'random', () => 0.99);
    const [, counter] = chooseCell([], area, 3, area[0], 0);
    assert.equal(counter, 4);
});
