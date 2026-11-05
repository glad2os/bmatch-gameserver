const {test} = require('node:test');
const assert = require('node:assert/strict');
const {generateArea} = require('../gameLogic/areaGenerator');

test('shuffled board contains exactly two copies of each value 1..50', () => {
    for (const pickIndex of [() => 0, max => max - 1, undefined]) {
        const area = generateArea(pickIndex);
        assert.equal(area.length, 100);
        for (let value = 1; value <= 50; value++) assert.equal(area.filter(v => v === value).length, 2);
    }
    assert.notDeepEqual(generateArea(() => 0), generateArea(max => max - 1));
});

test('Fisher-Yates places every image at every position about equally often', () => {
    // Over many boards, the share of games where cell 0 holds image 1 should be close to 2/100.
    const runs = 20000;
    let hits = 0;
    for (let i = 0; i < runs; i++) if (generateArea()[0] === 1) hits++;
    const share = hits / runs;
    assert.ok(share > 0.015 && share < 0.025, `share ${share}`);
});
