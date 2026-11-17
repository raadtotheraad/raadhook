"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.log = log;
exports.warn = warn;
exports.error = error;
// basic logger with timestamps
const white = '\u001b[30;47m';
const yellow = '\u001b[30;43m';
const red = '\u001b[37;41m';
function log(...args) {
    console.log(white, new Date().toISOString(), '\u001b[0m', white, 'i', '\u001b[0m', ...args);
}
function warn(...args) {
    console.warn(yellow, new Date().toISOString(), '\u001b[0m', yellow, '!', '\u001b[0m', ...args);
}
function error(...args) {
    console.error(red, new Date().toISOString(), '\u001b[0m', red, '!!', '\u001b[0m', ...args);
}
//# sourceMappingURL=log.js.map