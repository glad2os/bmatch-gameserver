const {test} = require('node:test');
const assert = require('node:assert/strict');
const {validateMove} = require('../gameLogic/validateMove');
const action = {openBubbles: [{bubbleId: '10'}], requestedBubbles: {bubbleId: '3'}};
test('accepts integer IDs including zero and rejects malformed/out-of-range IDs', () => {
    for (const id of [0, '0', 99, '42']) assert.equal(validateMove(id, action, 2), true);
    for (const id of [undefined, null, '', ' ', false, [], {}, -1, 100, 1.5, NaN, Infinity, 'x']) {
        assert.equal(validateMove(id, action, 2), false, String(id));
    }
});
test('rejects already matched bubbles and the first bubble of the same turn', () => {
    assert.equal(validateMove(10, action, 2), false);
    assert.equal(validateMove(3, action, 1), false);
    assert.equal(validateMove(3, action, 2), true);
    assert.equal(validateMove(4, action, 1), true);
    assert.equal(validateMove(4, action, 0), false);
});
