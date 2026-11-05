const {randomInt} = require('node:crypto');

// Uniform random integer in [0, max). Uses the CSPRNG so a board cannot be predicted from earlier ones.
const secureIndex = max => randomInt(max);

// The board: images 1..50, each twice, shuffled with Fisher-Yates (every order equally likely).
// pickIndex(max) returns an integer in [0, max); tests pass a deterministic one.
function generateArea(pickIndex = secureIndex) {
    let list = [];
    for(let i = 1; i <= 50; i++) {
        list.push(i, i);
    }
    for (let i = list.length - 1; i > 0; i--) {
        const j = pickIndex(i + 1);
        [list[i], list[j]] = [list[j], list[i]];
    }
    return list;
}


module.exports = {
    generateArea
}
