#! /usr/bin/env node

const Promise = require("bluebird");
const { MongoClient } = require("mongodb");
const { program } = require("commander");
const log = require("single-line-log").stdout;
const cliProgress = require("cli-progress");

let currData = 0;

const loadDbFromUrl = async (mongoUrl) => {
  try {
    const dbName = mongoUrl.split("/").pop();

    const client = new MongoClient(mongoUrl);

    await client
      .connect()
      .catch(`🚫  Auth error! Please double check source and target URL's`);

    return client.db(dbName);
  } catch (e) {
    console.error("\x1b[31m%s\x1b[0m", "🚫  Failed to connect to DB! ", e);
    process.exit(1);
  }
};

const copyCollection = async (source, target, name, bar) => {
  try {
    return new Promise(async (res, rej) => {
      const sourceCollection = await source.collection(name);
      const targetCollection = await target.collection(name);
      const allData = await sourceCollection.find().toArray();

      await Promise.all(
        allData.map(async (d) => {
          try {
            if (currData === 0) {
              bar.progress.start(bar.globalCountOfData, 0);
            }

            await targetCollection.insertOne(d);

            bar.progress.update(++currData, {
              speed: name,
            });
            if (currData === bar.globalCountOfData) {
              bar.progress.update(currData, {
                speed: "DONE",
              });
              return res(bar.progress.stop(), process.exit(0));
            }
          } catch (e) {
            console.error(
              "\x1b[31m%s\x1b[0m",
              "\n🚫  Error inserting in the new collection! Probably duplicated data is already inside new DB."
            );
            return rej(process.exit(1));
          }
        })
      );
    });
  } catch (e) {
    console.error("\x1b[31m%s\x1b[0m", "🚫  Error copying the collection!");
    process.exit(1);
  }
};

const main = async (sourceDbUrl, targetDbUrl, forceDrop) => {
  return new Promise(async (res, rej) => {
    try {
      const clientSource = await loadDbFromUrl(sourceDbUrl);
      const clientTarget = await loadDbFromUrl(targetDbUrl);
      const collections = await clientSource.listCollections().toArray();
      const progress = new cliProgress.Bar(
        {
          format:
            "📦  [{bar}] {percentage}% | ETA: {eta}s | {value}/{total} | Cloning: {speed}",
        },
        cliProgress.Presets.rect
      );
      let globalCountOfData = 0;
      await Promise.all(
        collections.map(async (c) => {
          const sourceCollection = await clientSource.collection(c.name);
          let count = 0;
          if (c.name !== "system.indexes") {
            count = await sourceCollection.countDocuments();
            log(`🔻 Fetching: ${c.name}`);
          }
          globalCountOfData += count;
        })
      );
      await log(`🔻 Fetching: DONE`);

      if (forceDrop) {
        console.log(clientTarget.databaseName);
        console.log("🗑 Drop target database: ", clientTarget.databaseName);
        await clientTarget.dropDatabase();
        console.log();
      }
      await Promise.all(
        collections.map(async (c) => {
          if (c.name !== "system.indexes") {
            await copyCollection(clientSource, clientTarget, c.name, {
              progress,
              globalCountOfData,
            });
          }
        })
      );
      res();
    } catch (e) {
      console.error(
        "\x1b[31m%s\x1b[0m",
        "🚫  Error copying the collection!\n",
        e
      );
      rej(e);
      process.exit(1);
    }
  });
};

(async () => {
  try {
    program
      .version("1.4.0")
      .usage("-s <SOURCE_MONGO_DB_URL> -t <TARGET_MONGO_DB_URL> [-f]")
      .option("-s, --source <sourceUrl>", "The source Mongo URL")
      .requiredOption("-t, --target <targetUrl>", "The target Mongo URL")
      .option("-f, --force", "Delete (drop) the target database before cloning")
      .exitOverride();

    program.parse();

    const options = program.opts();

    if (!options.source || !options.target) {
      console.log("\x1b[31m%s\x1b[0m", "🚫  Error: Please include arguments!");
      console.log(
        "\x1b[33m%s\x1b[0m",
        "ℹ️  USAGE: mongo-clone -s <SOURCE_MONGO_DB_URL> -t <TARGET_MONGO_DB_URL>"
      );
      console.log(
        "\x1b[33m%s\x1b[0m",
        "ℹ️  MongoURL example: mongodb://USER:PASS@HOST:PORT/DBNAME"
      );
      console.log(
        "\x1b[36m%s\x1b[0m",
        "🐛  If you have questions/suggestions/bug to report, ping me on fr1sk@live.com"
      );
      process.exit(1);
    }

    await main(options.source, options.target, options.force);
  } catch (e) {
    console.error(e);
    if (e.exitCode === 1)
      console.log("\x1b[31m%s\x1b[0m", "🚫  DB not cloned! 😢");
    if (e.exitCode === 0)
      console.log("\x1b[32m%s\x1b[0m", "🎉  DB cloned successfully!");
    process.exit(1);
  }
})();
