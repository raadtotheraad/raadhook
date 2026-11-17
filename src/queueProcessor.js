"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const ioredis_1 = __importDefault(require("ioredis"));
const fs_1 = __importDefault(require("fs"));
const beforeShutdown_1 = __importDefault(require("./beforeShutdown"));
const log_1 = require("./log");
const rmq_1 = require("./rmq");
const config = JSON.parse(fs_1.default.readFileSync('./config.json', 'utf8'));
const redis = new ioredis_1.default(config.redis);
const client = axios_1.default.create({
    validateStatus: () => true
});
let rabbitMq;
async function run() {
    try {
        rabbitMq = await (0, rmq_1.setup)(config.queue.rabbitmq, config.queue.queue);
        (0, beforeShutdown_1.default)(async () => {
            await rabbitMq.close();
        });
        (0, log_1.log)('RabbitMQ set up.');
    }
    catch (e) {
        (0, log_1.error)('RabbitMQ init error:', e);
        process.exit(-1);
    }
    (0, log_1.log)('Consuming.');
    await rabbitMq.prefetch(10);
    await rabbitMq.consume(config.queue.queue, async (msg) => {
        const data = JSON.parse(msg.content.toString());
        // since the actual proxy sets this key, this is a more reliable way of checking the ratelimit
        if (parseInt(await redis.get(`webhookRatelimit:${data.id}`)) === 0) {
            // mark message as dead, will be requeued via DLX
            return rabbitMq.reject(msg);
        }
        let response;
        try {
            response = await client.post(`http://localhost:${config.port}/api/webhooks/${data.id}/${data.token}?wait=false${data.threadId ? '&thread_id=' + data.threadId : ''}`, data.body, {
                headers: {
                    'User-Agent': 'WebhookProxy-QueueProcessor/1.0 (https://github.com/lewisakura/webhook-proxy)',
                    'Content-Type': 'application/json'
                }
            });
        }
        catch (e) {
            (0, log_1.error)('Failed to submit webhook to self:', e);
            return rabbitMq.reject(msg);
        }
        // can be ratelimited due to concurrency (e.g., sending webhooks manually + queueing)
        if (response.status === 429)
            return rabbitMq.reject(msg); // die if ratelimited
        if (response.status >= 400 && response.status < 500) {
            (0, log_1.warn)(data.id, 'made a bad request');
        }
        rabbitMq.ack(msg);
    }, { noAck: false });
}
run();
//# sourceMappingURL=queueProcessor.js.map