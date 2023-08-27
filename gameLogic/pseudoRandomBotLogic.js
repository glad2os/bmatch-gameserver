
function pseudoRandomChoice(attemptsSinceLastSuccess, lastSuccessfulAttempt) {
    const C = 0.05;
    const probability = C * (attemptsSinceLastSuccess + 1);

    const randomValue = Math.random();

    if (randomValue <= probability) {
        // if true level above set lastSuccessfulAttempt to 0
        return true;
    }

    lastSuccessfulAttempt++;
    return false;
}

function chooseCell(openedCells, allCells, lastSuccessfulAttempt) {
    const availableCells = allCells.filter(cell => !openedCells.includes(cell));

    let attempts = 0;

    while (!pseudoRandomChoice(lastSuccessfulAttempt)) {
        attempts++;
        if (attempts > availableCells.length) {
            break;
        }
    }

    return availableCells[Math.floor(Math.random() * availableCells.length)];
}

module.exports = {
    pseudoRandomChoice, chooseCell
}