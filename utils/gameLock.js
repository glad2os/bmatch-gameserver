// Single-process ordering only. Multiple replicas need a distributed lock.
const pending = new Map();
function withGameLock(gameUUID, action) {
    const previous = pending.get(gameUUID) || Promise.resolve();
    const result = previous.catch(() => {}).then(action);
    const tail = result.catch(() => {});
    pending.set(gameUUID, tail);
    return result.finally(() => {
        if (pending.get(gameUUID) === tail) pending.delete(gameUUID);
    });
}
module.exports = {withGameLock};
