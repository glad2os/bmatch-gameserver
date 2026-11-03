const {test} = require('node:test');
const assert = require('node:assert/strict');
const {generateArea} = require('../gameLogic/areaGenerator');
test('shuffled board contains exactly two copies of each value 1..50', () => {
    for (const random of [() => 0, () => 0.9999, Math.random]) {
        const area = generateArea(random);
        assert.equal(area.length, 100);
        for (let value = 1; value <= 50; value++) assert.equal(area.filter(v => v === value).length, 2);
    }
    assert.notDeepEqual(generateArea(() => 0), generateArea(() => 0.9999));
});
