"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const body_parser_1 = __importDefault(require("body-parser"));
const express_1 = __importDefault(require("express"));
const express_slow_down_1 = __importDefault(require("express-slow-down"));
const rate_limit_redis_1 = __importDefault(require("rate-limit-redis"));
const client_1 = require("@prisma/client");
const ioredis_1 = __importDefault(require("ioredis"));
const crypto_1 = __importDefault(require("crypto"));
const fs_1 = __importDefault(require("fs"));
const https_1 = __importDefault(require("https"));
const os_1 = __importDefault(require("os"));
const beforeShutdown_1 = __importDefault(require("./beforeShutdown"));
const log_1 = require("./log");
const robloxRanges_1 = require("./robloxRanges");
require("express-async-errors");
const rmq_1 = require("./rmq");
const VERSION = (() => {
    const rev = fs_1.default.readFileSync('.git/HEAD').toString().trim();
    if (rev.indexOf(':') === -1) {
        return rev;
    }
    else {
        return fs_1.default
            .readFileSync('.git/' + rev.substring(5))
            .toString()
            .trim()
            .slice(0, 7);
    }
})();
const app = (0, express_1.default)();
const config = JSON.parse(fs_1.default.readFileSync('./config.json', 'utf8'));
const db = new client_1.PrismaClient();
const redis = new ioredis_1.default(config.redis);
(0, beforeShutdown_1.default)(async () => {
    await db.$disconnect();
    redis.disconnect(false);
});
const axiosClients = [];
let currentRobin = 0;
function client() {
    const instance = axiosClients[currentRobin];
    currentRobin++;
    if (currentRobin === axiosClients.length)
        currentRobin = 0;
    return instance;
}
let currentSafeRobin = 0;
async function clientSafe(startedAt = null) {
    const instance = axiosClients[currentSafeRobin];
    currentSafeRobin++;
    if (startedAt === currentSafeRobin)
        return null; // means there is no suitable client
    if (currentSafeRobin === axiosClients.length)
        currentSafeRobin = 0;
    if (parseInt(await redis.get(`clientAbuse:${instance[1]}`)) >= config.abuseThreshold)
        return clientSafe(currentSafeRobin);
    return instance;
}
async function getClient(webhookId) {
    if (!(await redis.get(`webhooksSeen:${webhookId}`)) || (await redis.get(`webhooksSeen:${webhookId}`)) === 'false') {
        return clientSafe();
    }
    else {
        return client();
    }
}
for (const [_, iface] of Object.entries(os_1.default.networkInterfaces())) {
    for (const net of iface) {
        if (net.internal || net.family !== 'IPv4')
            continue;
        axiosClients.push([
            axios_1.default.create({
                httpsAgent: new https_1.default.Agent({
                    // @ts-ignore - undocumented
                    localAddress: net.address
                }),
                headers: {
                    'User-Agent': 'WebhookProxy/1.0 (https://github.com/lewisakura/webhook-proxy)'
                },
                validateStatus: () => true
            }),
            net.address
        ]);
        (0, log_1.log)('Discovered IP address', net.address);
    }
}
let rabbitMq;
let requestsHandled = 0;
async function banWebhook(id, reason, gameId) {
    // set the cached version up first so we prevent race conditions.
    //
    // without setting the cache first, we might hit a point where two requests trigger a ban.
    // setting it in cache first will prevent this since it will read the cached version first,
    // realise that they're banned, and stop the request there.
    await redis.set(`webhookBan:${id}`, reason, 'EX', 24 * 60 * 60);
    await db.bannedWebhook.upsert({
        where: {
            id
        },
        create: {
            id,
            reason
        },
        update: {
            reason
        }
    });
    (0, log_1.warn)('banned', formatId(id, gameId), 'for', reason);
}
async function banIp(ip, reason) {
    // see justification above for setting cache first
    const expiry = new Date();
    expiry.setDate(expiry.getDate() + 3); // 3-day ban
    // generate a hash for redis since IPv6 is a pain to store in redis
    const hash = crypto_1.default.createHash('sha1').update(ip).digest('hex');
    await redis.set(`ipBan:${hash}`, JSON.stringify({ reason, expires: expiry }), "PXAT", expiry.getTime());
    await db.bannedIP.upsert({
        where: {
            id: ip
        },
        create: {
            id: ip,
            reason,
            expires: expiry
        },
        update: {
            reason
        }
    });
    (0, log_1.warn)('banned', ip, 'for', reason);
}
async function trackBadRequest(id, gameId) {
    const violations = await redis.incr(`badRequests:${id}`);
    await redis.send_command('EXPIRE', [`badRequests:${id}`, 600, 'NX']);
    (0, log_1.warn)(formatId(id, gameId), 'made a bad request, they have made', violations, 'within the window');
    if (violations > 30 && config.autoBlock) {
        await banWebhook(id, '[Automated] >30 bad requests within 10 minutes.');
        await redis.del(`badRequests:${id}`);
    }
    return violations;
}
async function trackNonExistentWebhook(ip, clientAddress) {
    if (ip === 'localhost' || ip === '::1' || ip === '127.0.0.1' || ip === '::ffff:127.0.0.1')
        return; //ignore ourselves
    // generate a hash for redis since IPv6 is a pain to store in redis
    const hash = crypto_1.default.createHash('sha1').update(ip).digest('hex');
    const violations = await redis.incr(`nonExistentWebhooks:${hash}`);
    await redis.send_command('EXPIRE', [`nonExistentWebhooks:${hash}`, 3600, 'NX']);
    await redis.incr('nonExistentWebhooks');
    await redis.send_command('EXPIRE', ['nonExistentWebhooks', 86400, 'NX']);
    (0, log_1.warn)(ip, 'made a request to a nonexistent webhook, they have done so', violations, 'time within the window');
    await redis.incr(`clientAbuse:${clientAddress}`);
    await redis.send_command('EXPIRE', [`clientAbuse:${clientAddress}`, 86400, 'NX']);
    if (violations > 2 && config.autoBlock) {
        await banIp(ip, '[Automated] >2 unique non-existent webhook requests within 1 hour.');
        await redis.del(`nonExistentWebhooks:${hash}`);
    }
    return violations;
}
async function trackInvalidWebhookToken(ip) {
    if (ip === 'localhost' || ip === '::1' || ip === '127.0.0.1' || ip === '::ffff:127.0.0.1')
        return; //ignore ourselves
    // generate a hash for redis since IPv6 is a pain to store in redis
    const hash = crypto_1.default.createHash('sha1').update(ip).digest('hex');
    const violations = await redis.incr(`invalidWebhookToken:${hash}`);
    await redis.send_command('EXPIRE', [`invalidWebhookToken:${hash}`, 3600, 'NX']);
    await redis.incr('invalidWebhookToken');
    await redis.send_command('EXPIRE', ['invalidWebhookToken', 86400, 'NX']);
    (0, log_1.warn)(ip, 'made a request to a webhook with an invalid token, they have done so', violations, 'times within the window');
    if (violations > 10 && config.autoBlock) {
        await banIp(ip, '[Automated] >10 invalid webhook token requests within 1 hour.');
        await redis.del(`invalidWebhookToken:${hash}`);
    }
    return violations;
}
async function getWebhookBanInfo(id) {
    const data = await redis.get(`webhookBan:${id}`);
    if (data) {
        return data;
    }
    const ban = await db.bannedWebhook.findUnique({
        where: {
            id
        }
    });
    await redis.set(`webhookBan:${id}`, ban?.reason, 'EX', 24 * 60 * 60);
    return ban?.reason;
}
async function getGameBanInfo(id) {
    const data = await redis.get(`gameBan:${id}`);
    if (data) {
        return data;
    }
    const ban = await db.bannedGame.findUnique({
        where: {
            id
        }
    });
    await redis.set(`gameBan:${id}`, ban?.reason, 'EX', 24 * 60 * 60);
    return ban?.reason;
}
async function getIPBanInfo(ip) {
    if (ip === 'localhost' || ip === '::1' || ip === '127.0.0.1' || ip === '::ffff:127.0.0.1')
        return undefined; //ignore ourselves
    // generate a hash for redis since IPv6 is a pain to store in redis
    const hash = crypto_1.default.createHash('sha1').update(ip).digest('hex');
    const data = await redis.get(`ipBan:${hash}`);
    if (data) {
        const ban = JSON.parse(data);
        if (ban === null)
            return undefined;
        return { reason: ban.reason, expires: new Date(ban.expires) };
    }
    const ban = await db.bannedIP.findUnique({
        where: {
            id: ip
        },
        select: {
            reason: true,
            expires: true
        }
    });
    if (ban) {
        if (ban.expires.getTime() <= Date.now()) {
            await db.bannedIP.delete({
                where: {
                    id: ip
                }
            });
            await redis.del(`ipBan:${hash}`);
            return undefined;
        }
    }
    await redis.set(`ipBan:${hash}`, JSON.stringify(ban), 'PXAT', ban?.expires.getTime() ?? Date.now() + 24 * 60 * 60 * 1000);
    return ban;
}
function formatId(id, gameId) {
    if (gameId) {
        return `${id} (belonging to ${gameId})`;
    }
    else {
        return id;
    }
}
app.set('trust proxy', config.trustProxy);
app.use(require('helmet')({
    contentSecurityPolicy: false
}));
app.use(body_parser_1.default.json());
// catch spammers that ignore ratelimits in a way that can cause servers to yield for long periods of time
const webhookPostRatelimit = (0, express_slow_down_1.default)({
    windowMs: 2000,
    delayAfter: 5,
    delayMs: 1000,
    maxDelayMs: 30000,
    keyGenerator(req, res) {
        return req.params.id ?? req.ip; // use the webhook ID as a ratelimiting key, otherwise use IP
    },
    store: new rate_limit_redis_1.default({ client: redis, prefix: 'ratelimit:webhookPost:' })
});
const webhookQueuePostRatelimit = (0, express_slow_down_1.default)({
    windowMs: 1000,
    delayAfter: 10,
    delayMs: 1000,
    maxDelayMs: 30000,
    keyGenerator(req, res) {
        return req.params.id ?? req.ip; // use the webhook ID as a ratelimiting key, otherwise use IP
    },
    store: new rate_limit_redis_1.default({ client: redis, prefix: 'ratelimit:webhookQueue:' })
});
const webhookInvalidPostRatelimit = (0, express_slow_down_1.default)({
    windowMs: 30000,
    delayAfter: 3,
    delayMs: 1000,
    maxDelayMs: 30000,
    keyGenerator(req, res) {
        return req.params.id ?? req.ip; // use the webhook ID as a ratelimiting key, otherwise use IP
    },
    skip(req, res) {
        return !(res.statusCode >= 400 && res.statusCode < 500 && res.statusCode !== 429); // trigger if it's a 4xx but not a ratelimit
    },
    store: new rate_limit_redis_1.default({ client: redis, prefix: 'ratelimit:webhookInvalidPost:' })
});
const unknownEndpointRatelimit = (0, express_slow_down_1.default)({
    windowMs: 10000,
    delayAfter: 10,
    delayMs: 500,
    maxDelayMs: 30000,
    store: new rate_limit_redis_1.default({ client: redis, prefix: 'ratelimit:unknownEndpoint:' })
});
const statsEndpointRatelimit = (0, express_slow_down_1.default)({
    windowMs: 5000,
    delayAfter: 1,
    delayMs: 500,
    maxDelayMs: 30000,
    store: new rate_limit_redis_1.default({ client: redis, prefix: 'ratelimit:statsEndpoint:' })
});
app.use(express_1.default.static('public'));
app.get('/stats', statsEndpointRatelimit, async (req, res) => {
    const data = await Promise.all([
        (async () => parseInt((await redis.get('stats:requests')) ?? '0'))(),
        db.webhooksSeen.count()
    ]);
    return res.json({
        requests: data[0],
        webhooks: data[1],
        version: VERSION
    });
});
app.get('/announcement', async (req, res) => {
    const announcement = await redis.hgetall('announcement');
    if (!announcement.style) {
        return res.json({});
    }
    return res.json({
        title: announcement['title'],
        message: announcement['message'],
        style: announcement['style']
    });
});
// sure this could be middleware but I want better control
async function preRequestChecks(req, res, gameId) {
    const ipBan = await getIPBanInfo(req.ip);
    if (ipBan) {
        (0, log_1.warn)('ip', req.ip, 'attempted to request to', req.params.id, 'whilst banned');
        res.status(403).json({
            proxy: true,
            message: 'This IP address has been banned.',
            reason: ipBan.reason,
            expires: ipBan.expires.getTime()
        });
        return false;
    }
    if (gameId) {
        const gameBan = await getGameBanInfo(gameId);
        if (gameBan) {
            (0, log_1.warn)('game', gameId, 'attempted to request to', req.params.id, 'whilst banned');
            res.status(403).json({
                proxy: true,
                message: 'This game has been banned.',
                reason: gameBan
            });
            return false;
        }
    }
    const banInfo = await getWebhookBanInfo(req.params.id);
    if (banInfo) {
        (0, log_1.warn)(formatId(req.params.id, gameId), 'attempted to request whilst blocked for', banInfo);
        res.status(403).json({
            proxy: true,
            message: 'This webhook has been blocked. Please contact @lewisakura on the DevForum.',
            reason: banInfo
        });
        return false;
    }
    // if we know this webhook is already ratelimited, don't hit discord but reject the request instead
    const ratelimit = parseInt(await redis.get(`webhookRatelimit:${req.params.id}`));
    if (ratelimit === 0) {
        // get the timestamp for reset
        const ttl = Math.floor(Date.now() / 1000) + await redis.ttl(`webhookRatelimit:${req.params.id}`);
        res.setHeader('X-RateLimit-Limit', 5);
        res.setHeader('X-RateLimit-Remaining', 0);
        res.setHeader("X-RateLimit-Reset", ttl);
        res.status(429).json({
            proxy: true,
            message: 'You have been ratelimited. Please respect the standard Discord ratelimits.'
        });
        return false;
    }
    if (!(await redis.exists(`webhooksSeen:${req.params.id}`))) {
        await redis.set(`webhooksSeen:${req.params.id}`, (!!(await db.webhooksSeen.findFirst({ where: { id: req.params.id } }))).toString());
        await redis.send_command('EXPIRE', [`webhooksSeen:${req.params.id}`, 600, 'NX']);
    }
    return true;
}
async function postRequestChecks(req, res, response, clientAddress, gameId) {
    if (response.status === 401 && response.data.code === 50027 /* invalid webhook token */) {
        await trackInvalidWebhookToken(req.ip);
        res.status(401).json({
            proxy: true,
            error: 'The authorization token for this webhook is invalid.'
        });
        return false;
    }
    if (response.status === 404 && response.data.code === 10015 /* webhook not found */) {
        await db.bannedWebhook.upsert({
            where: {
                id: req.params.id
            },
            create: {
                id: req.params.id,
                reason: '[Automated] Webhook does not exist.'
            },
            update: {
                reason: '[Automated] Webhook does not exist.'
            }
        });
        await trackNonExistentWebhook(req.ip, clientAddress);
        res.status(404).json({
            proxy: true,
            error: 'This webhook does not exist.'
        });
        return false;
    }
    // new webhook!
    if (!(await redis.exists(`webhooksSeen:${req.params.id}`)) ||
        (await redis.get(`webhooksSeen:${req.params.id}`)) === 'false') {
        await redis.set(`webhooksSeen:${req.params.id}`, 'true');
        await redis.send_command('EXPIRE', [`webhooksSeen:${req.params.id}`, 600, 'NX']);
        await db.webhooksSeen.upsert({ where: { id: req.params.id }, update: {}, create: { id: req.params.id } });
    }
    if (response.status >= 400 && response.status < 500 && response.status !== 429) {
        await trackBadRequest(req.params.id, gameId);
    }
    // process ratelimits
    await redis.set(`webhookRatelimit:${req.params.id}`, response.headers['x-ratelimit-remaining'], 'EXAT', parseInt(response.headers['x-ratelimit-reset']));
    return true;
}
app.post('/api/webhooks/:id/:token', webhookPostRatelimit, webhookInvalidPostRatelimit, async (req, res) => {
    redis.incr('stats:requests');
    requestsHandled++;
    try {
        BigInt(req.params.id);
    }
    catch {
        res.status(400).json({
            proxy: true,
            error: 'Webhook ID does not appear to be a snowflake.'
        });
        return false;
    }
    const gameId = robloxRanges_1.robloxRanges.check(req.ip) ? req.header('roblox-id') : undefined;
    if (!(await preRequestChecks(req, res, gameId)))
        return;
    const body = req.body;
    if (!body.content && !body.embeds && !body.file) {
        res.status(400).json({
            proxy: true,
            error: 'No body provided. The proxy only accepts valid JSON bodies.'
        });
        return false;
    }
    const wait = req.query.wait ?? false;
    const threadId = req.query.thread_id;
    const axios = await getClient(req.params.id);
    if (!axios) {
        res.status(403).json({
            proxy: true,
            error: 'The proxy has not seen your webhook before, and is currently unable to service your request.'
        });
        return false;
    }
    const response = await axios[0].post(`https://discord.com/api/webhooks/${req.params.id}/${req.params.token}?wait=${wait}${threadId ? '&thread_id=' + threadId : ''}`, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    });
    if (!(await postRequestChecks(req, res, response, axios[1], gameId)))
        return;
    // forward headers to allow clients to process ratelimits themselves
    for (const header of Object.keys(response.headers)) {
        res.setHeader(header, response.headers[header]);
    }
    res.removeHeader('Transfer-Encoding'); // the proxy changes how this is encoded, so it's wrong to actually include this header even if Discord does
    res.setHeader('Via', '1.0 WebhookProxy');
    return res.status(response.status).json(response.data);
});
// PATCHes use the same ratelimit bucket as the regular message endpoint, so we don't do any special ratelimit handling here.
app.patch('/api/webhooks/:id/:token/messages/:messageId', webhookPostRatelimit, webhookInvalidPostRatelimit, async (req, res) => {
    redis.incr('stats:requests');
    requestsHandled++;
    try {
        BigInt(req.params.id);
    }
    catch {
        res.status(400).json({
            proxy: true,
            error: 'Webhook ID does not appear to be a snowflake.'
        });
        return false;
    }
    try {
        BigInt(req.params.messageId);
    }
    catch {
        return res.status(400).json({
            proxy: true,
            error: 'Message ID does not appear to be a snowflake.'
        });
    }
    const gameId = robloxRanges_1.robloxRanges.check(req.ip) ? req.header('roblox-id') : undefined;
    if (!(await preRequestChecks(req, res, gameId)))
        return;
    const body = req.body;
    if (!body.content && !body.embeds && !body.file) {
        res.status(400).json({
            proxy: true,
            error: 'No body provided. The proxy only accepts valid JSON bodies.'
        });
        return false;
    }
    const threadId = req.query.thread_id;
    const axios = await getClient(req.params.id);
    if (!axios) {
        res.status(403).json({
            proxy: true,
            error: 'The proxy has not seen your webhook before, and is currently unable to service your request.'
        });
        return false;
    }
    const response = await axios[0].patch(`https://discord.com/api/webhooks/${req.params.id}/${req.params.token}/messages/${req.params.messageId}${threadId ? '?thread_id=' + threadId : ''}`, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    });
    if (!(await postRequestChecks(req, res, response, axios[1], gameId)))
        return;
    // forward headers to allow clients to process ratelimits themselves
    for (const header of Object.keys(response.headers)) {
        res.setHeader(header, response.headers[header]);
    }
    res.removeHeader('Transfer-Encoding'); // the proxy changes how this is encoded, so it's wrong to actually include this header even if Discord does
    res.setHeader('Via', '1.0 WebhookProxy');
    return res.status(response.status).json(response.data);
});
// DELETEs use the same ratelimit bucket as the regular message endpoint, so we don't do any special ratelimit handling here.
app.delete('/api/webhooks/:id/:token/messages/:messageId', webhookPostRatelimit, webhookInvalidPostRatelimit, async (req, res) => {
    redis.incr('stats:requests');
    requestsHandled++;
    try {
        BigInt(req.params.id);
    }
    catch {
        res.status(400).json({
            proxy: true,
            error: 'Webhook ID does not appear to be a snowflake.'
        });
        return false;
    }
    try {
        BigInt(req.params.messageId);
    }
    catch {
        return res.status(400).json({
            proxy: true,
            error: 'Message ID does not appear to be a snowflake.'
        });
    }
    const gameId = robloxRanges_1.robloxRanges.check(req.ip) ? req.header('roblox-id') : undefined;
    if (!(await preRequestChecks(req, res, gameId)))
        return;
    const threadId = req.query.thread_id;
    const axios = await getClient(req.params.id);
    if (!axios) {
        res.status(403).json({
            proxy: true,
            error: 'The proxy has not seen your webhook before, and is currently unable to service your request.'
        });
        return false;
    }
    const response = await axios[0].delete(`https://discord.com/api/webhooks/${req.params.id}/${req.params.token}/messages/${req.params.messageId}${threadId ? '?thread_id=' + threadId : ''}`, {
        headers: {
            'Content-Type': 'application/json'
        }
    });
    if (!(await postRequestChecks(req, res, response, axios[1], gameId)))
        return;
    // forward headers to allow clients to process ratelimits themselves
    for (const header of Object.keys(response.headers)) {
        res.setHeader(header, response.headers[header]);
    }
    res.removeHeader('Transfer-Encoding'); // the proxy changes how this is encoded, so it's wrong to actually include this header even if Discord does
    res.setHeader('Via', '1.0 WebhookProxy');
    return res.status(response.status).json(response.data);
});
app.post('/api/webhooks/:id/:token/queue', webhookQueuePostRatelimit, async (req, res) => {
    if (!config.queue.enabled)
        return res.status(403).json({ proxy: true, error: 'Queues have been disabled.' });
    // run the same ban checks again so we don't hit ourselves if the webhook is bad
    const ipBan = await getIPBanInfo(req.ip);
    if (ipBan) {
        (0, log_1.warn)('ip', req.ip, 'attempted to queue to', req.params.id, 'whilst banned');
        return res.status(403).json({
            proxy: true,
            message: 'This IP address has been banned.',
            reason: ipBan.reason,
            expires: ipBan.expires.getTime()
        });
    }
    const gameId = robloxRanges_1.robloxRanges.check(req.ip) ? req.header('roblox-id') : undefined;
    const threadId = req.query.thread_id;
    const body = req.body;
    const reason = await getWebhookBanInfo(req.params.id);
    if (reason) {
        (0, log_1.warn)(formatId(req.params.id, gameId), 'attempted to queue whilst blocked for', reason);
        return res.status(403).json({
            proxy: true,
            message: 'This webhook has been blocked. Please contact @lewisakura on the DevForum.',
            reason: reason
        });
    }
    rabbitMq.sendToQueue(config.queue.queue, Buffer.from(JSON.stringify({
        id: req.params.id,
        token: req.params.token,
        body,
        threadId: threadId
    })), {
        persistent: true // make messages persistent to minimise lost messages
    });
    return res.json({
        proxy: true,
        message: 'Queued successfully.'
    });
});
app.use(unknownEndpointRatelimit, (req, res, next) => {
    (0, log_1.warn)(req.ip, 'hit unknown endpoint');
    return res.status(404).json({
        proxy: true,
        message: 'Unknown endpoint.'
    });
});
app.use((err, req, res, next) => {
    if (err instanceof SyntaxError && 'body' in err) {
        return res.status(400).json({
            proxy: true,
            error: 'Malformed request. The proxy only accepts valid JSON bodies.'
        });
    }
    (0, log_1.error)('error encountered:', err, 'by', req.params.id ?? req.ip);
    return res.status(500).json({
        proxy: true,
        error: 'An error occurred while processing your request.'
    });
});
app.listen(config.port, async () => {
    (0, log_1.log)('Up and running. Version:', VERSION);
    setInterval(() => {
        (0, log_1.log)('In the last minute, this worker handled', requestsHandled, 'requests.');
        requestsHandled = 0;
    }, 60000);
    if (config.queue.enabled) {
        try {
            rabbitMq = await (0, rmq_1.setup)(config.queue.rabbitmq, config.queue.queue);
            (0, beforeShutdown_1.default)(async () => {
                await rabbitMq.close();
            });
            (0, log_1.log)('RabbitMQ set up.');
        }
        catch (e) {
            (0, log_1.error)('RabbitMQ init error, will disable queues:', e);
            config.queue.enabled = false;
        }
    }
});
//# sourceMappingURL=index.js.map