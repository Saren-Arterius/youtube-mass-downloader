const Queue = require('bull');
const {exec} = require('mz/child_process');
const {writeFile} = require('mz/fs');
const {join} = require('path');
const config = require('./config');

const queue = new Queue(`${config.queue_name}`, {redis: config.redis});

queue.process(config.queue_name, async (job, done) => {
  try {
    const ytid = job.id;
    const rcloneVideoBase = join(config.rclone_base, ytid.substr(0, 2), ytid.substr(2));
    const downloadThumbnail = async () => {
      const path = join(rcloneVideoBase, 'thumbnail.jpg');
      console.log(`[${ytid}] Downloading thumbnail to ${path}...`);
      await exec(`youtube-dl 'https://www.youtube.com/watch?v=${ytid}' -q --write-thumbnail --skip-download -o /tmp/${ytid}.jpg`);
      await exec(`rclone copy /tmp/${ytid}.jpg ${path}`);
      await exec(`rm /tmp/${ytid}.jpg`);
      console.log(`[${ytid}] Thumbnail downloaded.`);
    };
    const downloadMeta = async () => {
      const path = join(rcloneVideoBase, 'info.json');
      console.log(`[${ytid}] Downloading meta to ${path}...`);
      const [stdout] = await exec(`youtube-dl 'https://www.youtube.com/watch?v=${ytid}' -q --dump-json`);
      const info = JSON.parse(stdout);
      delete info.requested_formats;
      delete info.formats;
      await job.update({
        title: info.title
      });
      await writeFile(`/tmp/${ytid}.json`, JSON.stringify(info));
      await exec(`rclone copy /tmp/${ytid}.json ${path}`);
      await exec(`rm /tmp/${ytid}.json`);
      console.log(`[${ytid}] Meta downloaded.`);
    };
    const downloadVideo = async () => {
      const path = join(rcloneVideoBase, 'video.mkv');
      console.log(`[${ytid}] Downloading video to ${path}...`);
      await exec(`youtube-dl 'https://www.youtube.com/watch?v=${ytid}' -q -o - | rclone rcat ${path}`);
      // await exec(`rclone copy /tmp/${ytid}.mkv ${path}`);
      // await exec(`rm /tmp/${ytid}.mkv`);
      console.log(`[${ytid}] Video downloaded.`);
    };
    await Promise.all([
      downloadThumbnail(),
      downloadMeta(),
      downloadVideo()
    ]);
  } catch (e) {
    console.log(e);
    done(e);
  }
  done();
});
