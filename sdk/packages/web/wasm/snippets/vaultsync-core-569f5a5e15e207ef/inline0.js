
export function acquire_web_lock(lock_name, lock_id, on_acquired, on_released) {
    let release_promise = new Promise((resolve) => {
        window._vaultsync_releases = window._vaultsync_releases || {};
        window._vaultsync_releases[lock_id] = resolve;
    });
    navigator.locks.request(lock_name, async (lock) => {
        on_acquired();
        await release_promise;
        on_released();
    }).catch(err => {
        console.error("Web Lock acquisition failed", err);
    });
}
export function acquire_lock_immediate(lock_name, lock_id, on_acquired, on_released) {
    let outer_resolve;
    let outer_promise = new Promise((resolve) => { outer_resolve = resolve; });
    navigator.locks.request(lock_name, { ifAvailable: true }, async (lock) => {
        if (lock === null) { outer_resolve(false); return; }
        on_acquired();
        outer_resolve(true);
        await new Promise((resolve) => {
            window._vaultsync_releases = window._vaultsync_releases || {};
            window._vaultsync_releases[lock_id] = resolve;
        });
        on_released();
    }).catch(err => {
        console.error("Web Lock acquisition failed", err);
        if (outer_resolve) outer_resolve(false);
    });
    return outer_promise;
}
export function release_web_lock(lock_id) {
    if (window._vaultsync_releases && window._vaultsync_releases[lock_id]) {
        window._vaultsync_releases[lock_id]();
        delete window._vaultsync_releases[lock_id];
    }
}
