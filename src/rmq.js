"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.setup = setup;
const amqplib_1 = __importDefault(require("amqplib"));
async function setup(host, queue) {
    const connection = await amqplib_1.default.connect(host);
    const channel = await connection.createChannel();
    // setup dead letter exchange for requeues
    await channel.assertExchange(queue, 'direct', { durable: true });
    await channel.assertExchange(queue + '-dead', 'direct', { durable: true });
    await channel.assertQueue(queue, { durable: true, deadLetterExchange: queue + '-dead' });
    await channel.assertQueue(queue + '-dead', { durable: true, deadLetterExchange: queue, messageTtl: 2000 });
    await channel.bindExchange(queue, queue, '');
    await channel.bindExchange(queue + '-dead', queue + '-dead', '');
    return channel;
}
//# sourceMappingURL=rmq.js.map