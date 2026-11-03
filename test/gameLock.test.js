const {test} = require('node:test');
const assert = require('node:assert/strict');
const {withGameLock} = require('../utils/gameLock');
test('serializes one game, permits another game and survives rejection', async () => {
    let release;
    const barrier = new Promise(resolve => { release = resolve; });
    const calls = [];
    const first = withGameLock('a', async () => { calls.push(1); await barrier; throw Error('failure'); });
    const failure = assert.rejects(first, /failure/);
    const second = withGameLock('a', () => calls.push(2));
    await withGameLock('b', () => calls.push('b'));
    assert.deepEqual(calls, [1, 'b']);
    release();
    await Promise.all([failure, second]);
    assert.deepEqual(calls, [1, 'b', 2]);
});
