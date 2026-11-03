const {test} = require('node:test');
const assert = require('node:assert/strict');
const {setTimer, cancelTimer, getRemainingTime} = require('../utils/getGameData');
const {setupRabbitMQ} = require('../sockets/gamews');
function redis() {
    const values = new Map([['Game:a', {}], ['Game:b', {}]]);
    return {
        exists: async key => values.has(key),
        hGet: async (key, field) => values.get(key)?.[field] ?? null,
        hGetAll: async key => values.get(key) || {},
        hSet: async (key, field, value) => {
            const data = values.get(key) || {};
            Object.assign(data, typeof field === 'object' ? field : {[field]: String(value)});
            values.set(key, data);
        },
        hIncrBy: async (key, field, amount) => {
            const data = values.get(key);
            data[field] = String(Number(data[field] || 0) + amount);
            return Number(data[field]);
        },
        ttl: async () => 21600,
        expire: async () => true
    };
}
test('timer cancellation invalidates only its game and honors duration', async () => {
    const client = redis();
    const messages = [];
    const channel = {publish: (exchange, route, content, options) => messages.push({exchange, route, body: JSON.parse(content), options})};
    await setTimer(channel, client, 'a', 7);
    await setTimer(channel, client, 'b', 30);
    assert.equal(messages[0].options.headers['x-delay'], 7000);
    assert.ok(await getRemainingTime(client, 'a') > 6);
    await cancelTimer(client, channel, 'a');
    assert.equal(await getRemainingTime(client, 'a'), 0);
    assert.notEqual(Number(await client.hGet('Game:a', 'TurnId')), messages[0].body.turnId);
    assert.equal(Number(await client.hGet('Game:b', 'TurnId')), messages[1].body.turnId);
    assert.ok(await getRemainingTime(client, 'b') > 29);
    await setTimer(channel, client, 'a', 4);
    assert.equal(messages[2].body.turnId, 3);
});
test('binds the delayed queue before any timer can be published', async () => {
    const calls = [];
    await setupRabbitMQ({
        assertExchange: async (...args) => calls.push(['exchange', ...args]),
        assertQueue: async (...args) => calls.push(['queue', ...args]),
        bindQueue: async (...args) => calls.push(['bind', ...args])
    });
    assert.equal(calls[0][2], 'x-delayed-message');
    assert.deepEqual(calls[2], ['bind', 'game_timer_fired', 'game_timer_delayed', 'fire']);
});
