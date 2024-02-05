import { Queue, QueueEvents, Worker } from 'bullmq';
import IORedis from 'ioredis';

const connection = new IORedis({ maxRetriesPerRequest: null });
const queue = new Queue('new-queue', { connection });

queue.add('test', { foo: 'bar' });
