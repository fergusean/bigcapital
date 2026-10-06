// Runs against disposable CI MySQL/Redis services, never a production endpoint.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const ts = require("typescript");
const Redis = require("ioredis");
const { Queue, Worker, QueueEvents } = require("bullmq");
const {
  ThrottlerStorageRedisService,
} = require("@nest-lab/throttler-storage-redis");
const knex = require("knex");
const mysql = require("mysql2/promise");

require.extensions[".ts"] = (module, filename) => {
  const source = fs.readFileSync(filename, "utf8");
  const output = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2021,
    },
  });
  module._compile(output.outputText, filename);
};
const configRoot = path.join(__dirname, "../packages/server/src/common/config");
const redisConfig = require(path.join(configRoot, "redis.ts")).default;
const queueConfig = require(path.join(configRoot, "queue.ts")).default;
const { redisConnectionOptions } = require(
  path.join(configRoot, "redis-connection.ts"),
);
const systemConfig = require(
  path.join(configRoot, "system-database.ts"),
).default;
const tenantConfig = require(
  path.join(configRoot, "tenant-database.ts"),
).default;

function checkConfiguration() {
  const saved = { ...process.env };
  try {
    for (const key of Object.keys(process.env)) {
      if (key.startsWith("REDIS_") || key.startsWith("QUEUE_"))
        delete process.env[key];
    }
    assert.equal(redisConfig().host, "localhost");
    assert.equal(redisConfig().port, 6379);
    assert.equal(redisConfig().keyPrefix, "");
    assert.equal(queueConfig().prefix, "bull");
    process.env.REDIS_USERNAME = "shared";
    process.env.REDIS_PASSWORD = "test-only";
    process.env.REDIS_DB = "2";
    process.env.REDIS_KEY_PREFIX = "bigcapital:";
    process.env.REDIS_SENTINELS = '[{"host":"sentinel","port":26379}]';
    const shared = redisConfig();
    assert.equal(shared.sentinelUsername, "shared");
    assert.equal(shared.sentinelPassword, "test-only");
    assert.equal(shared.name, "redis");
    const queue = queueConfig();
    assert.equal(queue.connection.username, "shared");
    assert.equal(queue.connection.password, "test-only");
    assert.equal(queue.connection.db, 2);
    assert.deepEqual(queue.connection.sentinels, shared.sentinels);
    assert.equal(queue.connection.maxRetriesPerRequest, null);
    assert.equal(queue.connection.keyPrefix, undefined);
    process.env.QUEUE_USERNAME = "queue-user";
    process.env.QUEUE_PASSWORD = "queue-only";
    process.env.QUEUE_PREFIX = "bigcapital:queues";
    assert.equal(queueConfig().connection.username, "queue-user");
    assert.equal(queueConfig().connection.password, "queue-only");
    assert.equal(queueConfig().prefix, "bigcapital:queues");
    for (const invalid of [
      "[]",
      '[{"host":"x","port":0}]',
      '[{"host":"x","port":"26379"}]',
    ]) {
      process.env.REDIS_SENTINELS = invalid;
      assert.throws(() => redisConnectionOptions("REDIS"));
    }
  } finally {
    process.env = saved;
  }
  console.log(
    "PASS: local defaults, ACL credentials, queue overrides, prefixes, Sentinel credentials, and invalid endpoints",
  );
}

async function checkRedis() {
  const admin = new Redis({ host: "127.0.0.1", port: 6379 });
  const clients = [];
  try {
    await admin.acl(
      "SETUSER",
      "bigcapital",
      "reset",
      "on",
      ">ci-only-password",
      "~bigcapital:*",
      "&bigcapital:*",
      "+@all",
      "-@admin",
    );
    process.env.REDIS_HOST = "127.0.0.1";
    process.env.REDIS_USERNAME = "bigcapital";
    process.env.REDIS_PASSWORD = "ci-only-password";
    process.env.REDIS_KEY_PREFIX = "bigcapital:";
    process.env.QUEUE_PREFIX = "bigcapital:queues";
    for (const sentinel of [false, true]) {
      if (sentinel) {
        process.env.REDIS_SENTINELS = '[{"host":"127.0.0.1","port":26379}]';
        process.env.REDIS_SENTINEL_USERNAME = "sentinel-client";
        process.env.REDIS_SENTINEL_PASSWORD = "ci-only-sentinel-password";
      }
      const client = new Redis(redisConfig());
      clients.push(client);
      await client.set("cache:test", "ok");
      const value = await client.get("cache:test");
      assert.equal(value, "ok");
      await assert.rejects(client.call("SET", "outside:key", "bad"), /NOPERM/);
      const throttle = new ThrottlerStorageRedisService(redisConfig());
      clients.push(throttle.redis);
      const first = await throttle.increment(
        `request-${sentinel}`,
        60000,
        1,
        60000,
        "auth",
      );
      const second = await throttle.increment(
        `request-${sentinel}`,
        60000,
        1,
        60000,
        "auth",
      );
      assert.equal(first.totalHits, 1);
      assert.equal(second.totalHits, 2);
      assert.equal(second.isBlocked, true);
      const options = queueConfig();
      const name = `compat-${sentinel}`;
      const queue = new Queue(name, options);
      const events = new QueueEvents(name, options);
      const worker = new Worker(
        name,
        async (job) => job.data.value + 1,
        options,
      );
      try {
        await events.waitUntilReady();
        const job = await queue.add("test", { value: 41 });
        const result = await job.waitUntilFinished(events, 15000);
        assert.equal(result, 42);
      } finally {
        await worker.close();
        await events.close();
        await queue.close();
      }
      console.log(
        `PASS: ${sentinel ? "Sentinel" : "direct"} ACL authentication, scoped cache, throttling Lua, queue worker and queue events`,
      );
    }
    const keys = await admin.keys("*");
    assert(keys.length > 0);
    assert(
      keys.every((key) => key.startsWith("bigcapital:")),
      "All Redis keys must stay inside the ACL prefix",
    );
  } finally {
    clients.forEach((client) => client.disconnect());
    admin.disconnect();
  }
}

async function checkMysql() {
  const admin = await mysql.createConnection({
    host: "127.0.0.1",
    user: "root",
    password: "ci-only-root-password",
  });
  const connections = [];
  try {
    await admin.query(
      "CREATE USER 'bigcapital'@'%' IDENTIFIED WITH caching_sha2_password BY 'ci-only-password'",
    );
    await admin.query("CREATE DATABASE bigcapital_system");
    await admin.query("CREATE DATABASE bigcapital_tenant_compat");
    await admin.query(
      "GRANT ALL PRIVILEGES ON bigcapital_system.* TO 'bigcapital'@'%'",
    );
    await admin.query(
      "GRANT ALL PRIVILEGES ON bigcapital_tenant_compat.* TO 'bigcapital'@'%'",
    );
    process.env.DB_HOST = "127.0.0.1";
    process.env.DB_USER = "bigcapital";
    process.env.DB_PASSWORD = "ci-only-password";
    process.env.SYSTEM_DB_NAME = "bigcapital_system";
    for (const config of [systemConfig(), tenantConfig()]) {
      assert.equal(config.client, "mysql2");
      const database = config.databaseName || "bigcapital_tenant_compat";
      const db = knex({
        client: config.client,
        connection: {
          host: config.host,
          port: config.port,
          user: config.user,
          password: config.password,
          database,
        },
      });
      connections.push(db);
      const result = await db.raw("SELECT DATABASE() AS name");
      assert.equal(result[0][0].name, database);
    }
    const tenant = connections[1];
    await tenant.schema.createTable("items", (table) => table.increments("id"));
    await tenant.schema.createTable("warehouses", (table) =>
      table.increments("id"),
    );
    const migration = require("../packages/server/src/database/tenant/migrations/20220125021920_create_items_warehouses_quantity.ts");
    await migration.up(tenant);
    await tenant("items").insert({ id: 1 });
    await tenant("warehouses").insert({ id: 1 });
    await tenant("items_warehouses_quantity").insert({
      item_id: 1,
      warehouse_id: 1,
      quantity_on_hand: 7,
    });
    const row = await tenant("items_warehouses_quantity").first();
    assert.equal(row.id, 1);
    assert.equal(row.quantity_on_hand, 7);
    await assert.rejects(
      tenant("items_warehouses_quantity").insert({
        item_id: 999,
        warehouse_id: 1,
      }),
    );
    await migration.down(tenant);
    console.log(
      "PASS: system and tenant mysql2 connections authenticate with caching_sha2_password; patched migration auto-increments and preserves foreign keys",
    );
  } finally {
    for (const db of connections) await db.destroy();
    await admin.end();
  }
}

async function main() {
  checkConfiguration();
  if (process.argv.includes("--config-only")) return;
  await checkRedis();
  await checkMysql();
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
