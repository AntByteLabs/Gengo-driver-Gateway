// Must import config first so Zod validation runs before anything else
import './config.js';
import { startServer, gracefulShutdown } from './server.js';
import { logger } from './logger.js';
startServer();
// ─── Graceful shutdown signals ────────────────────────────────────────────────
const shutdownSignals = ['SIGTERM', 'SIGINT'];
for (const signal of shutdownSignals) {
    process.once(signal, async () => {
        try {
            await gracefulShutdown(signal);
            process.exit(0);
        }
        catch (err) {
            logger.error({ err }, 'Error during graceful shutdown');
            process.exit(1);
        }
    });
}
process.on('uncaughtException', (err) => {
    logger.fatal({ err }, 'Uncaught exception — terminating');
    process.exit(1);
});
process.on('unhandledRejection', (reason) => {
    logger.fatal({ reason }, 'Unhandled promise rejection — terminating');
    process.exit(1);
});
//# sourceMappingURL=index.js.map