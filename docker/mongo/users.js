// Administrator credentials exist only in the offline bootstrap container.
try {
    const admin = db.getSiblingDB('admin');
    const env = process.env;
    let authenticated = false;
    try {
        authenticated = Boolean(admin.auth(env.MONGO_ROOT_USER, env.MONGO_ROOT_PASSWORD));
    } catch (_) {
        // The localhost exception permits only the first administrator on a fresh volume.
    }
    if (!authenticated) {
        admin.createUser({user: env.MONGO_ROOT_USER, pwd: env.MONGO_ROOT_PASSWORD,
            roles: [{role: 'root', db: 'admin'}]});
        if (!admin.auth(env.MONGO_ROOT_USER, env.MONGO_ROOT_PASSWORD)) {
            throw new Error('bootstrap authentication failed');
        }
    }
    for (const name of [env.MONGO_DB, env.MONGO_DB + '_test']) {
        const target = db.getSiblingDB(name);
        if (!target.getCollectionNames().includes('raw_events')) {
            target.createCollection('raw_events');
        }
        const collection = target.getCollection('raw_events');
        collection.createIndex({order_id: 1, event_ts: 1, batch_id: 1, source_file: 1, source_row_number: 1}, {name: 'order_timeline'});
        collection.createIndex({driver_id: 1, event_ts: 1, batch_id: 1, source_file: 1, source_row_number: 1}, {name: 'driver_timeline'});
        collection.createIndex({batch_id: 1}, {name: 'batch_lookup'});
        const roles = [
            {role: 'raw_ingest', privileges: [{resource: {db: name, collection: 'raw_events'}, actions: ['find', 'insert']}], roles: []},
            {role: 'raw_query', privileges: [
                {resource: {db: name, collection: 'raw_events'}, actions: ['find', 'listIndexes', 'collStats']},
                {resource: {db: name, collection: ''}, actions: ['dbStats']}
            ], roles: []}
        ];
        for (const role of roles) {
            if (target.getRole(role.role)) {
                target.updateRole(role.role, {privileges: role.privileges, roles: []});
            } else {
                target.createRole(role);
            }
        }
        for (const [prefix, role] of [['APP', 'raw_ingest'], ['READONLY', 'raw_query']]) {
            const user = env['MONGO_' + prefix + '_USER'];
            const properties = {pwd: env['MONGO_' + prefix + '_PASSWORD'], roles: [{role: role, db: name}]};
            if (target.getUser(user)) {
                target.updateUser(user, properties);
            } else {
                target.createUser({user: user, ...properties});
            }
        }
    }
} catch (_) {
    // Driver/server errors can contain command documents: never print them.
    print('ERROR: MongoDB bootstrap failed; verify configuration and volume ownership');
    quit(1);
}
