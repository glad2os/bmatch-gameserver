function generateArea(random = Math.random) {
    let list = [];
    for(let i = 1; i <= 50; i++) {
        list.push(i, i);
    }
    for (let i = list.length - 1; i > 0; i--) {
        const j = Math.floor(random() * (i + 1));
        [list[i], list[j]] = [list[j], list[i]];
    }
    return list;
}


module.exports = {
    generateArea
}