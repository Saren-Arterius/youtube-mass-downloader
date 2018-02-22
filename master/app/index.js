const Queue = require('bull');
const config = require('./config');

const queue = new Queue(config.queue_name, {redis: config.redis}); // Specify Redis connection using object

const lineReader = require('readline').createInterface({
  input: require('fs').createReadStream(config.youtube_ids_file)
});

lineReader.on('line', async (line) => {
  const id = line.trim();
  const job = await queue.getJob(id);
  if (!job) {
    await queue.add(config.queue_name, null, {
      jobId: id
    });
    console.log(`${id} queued`);
  }
});

queue.on('global:failed', (job, err) => {
  console.log(job, err);
});

setInterval(async () => {
  const counts = await queue.getJobCounts();
  console.log(`Remaining jobs: ${JSON.stringify(counts)}`);
}, 1000);

