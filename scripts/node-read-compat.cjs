'use strict';

const path = require('node:path');
const { createRequire } = require('node:module');

async function main() {
  const root = process.env.NODE_REFUND_ROOT;
  const encryptorRoot = process.env.NODE_ENCRYPTOR_ROOT;
  const id = process.env.COMPAT_BANK_DATA_ID;
  const expected = process.env.COMPAT_EXPECTED_VALUE;
  if (!root || !encryptorRoot || !id || !expected) throw new Error('missing compatibility test configuration');

  const nodeRequire = createRequire(path.join(root, 'package.json'));
  let pg;
  try {
    pg = nodeRequire('pg');
  } catch {
    // This workspace's package-manager migration retains the installed Node
    // driver under .ignored. It is still the exact package Node used.
    pg = nodeRequire(path.join(root, 'node_modules/.ignored/pg'));
  }
  const { Client } = pg;
  const encryptorRequire = createRequire(path.join(encryptorRoot, 'package.json'));
  const { ClientProxyFactory, Transport } = encryptorRequire('@nestjs/microservices');
  const { firstValueFrom, timeout } = encryptorRequire('rxjs');

  const database = new Client({ connectionString: process.env.DATABASE_URL });
  await database.connect();
  let row;
  try {
    const result = await database.query(
      `SELECT id, bank_code, account_number_enc, account_number_hash,
              account_status, account_result
         FROM bank_datas WHERE id = $1`,
      [id],
    );
    if (result.rowCount !== 1) throw new Error('Go-created row not found');
    row = result.rows[0];
    if (row.bank_code !== 'SYNTHETIC_COMPAT') throw new Error('bank code mismatch');
    if (row.account_status !== 'pending' || row.account_result !== 'pending') {
      throw new Error('state mapping mismatch');
    }
    if (!row.account_number_enc || row.account_number_enc.alg !== 'AES-256-GCM') {
      throw new Error('ciphertext mapping mismatch');
    }
  } finally {
    await database.end();
  }

  const broker = ClientProxyFactory.create({
    transport: Transport.RMQ,
    options: {
      urls: [process.env.RABBITMQ_URL],
      queue: process.env.RABBITMQ_ENCRYPTOR_QUEUE,
      queueOptions: { durable: true },
    },
  });
  try {
    const context = 'refund.bankData.accountNumber';
    const decrypted = await firstValueFrom(
      broker.send('decrypt', {
        payload: row.account_number_enc,
        aad: Buffer.from(context).toString('base64'),
        context,
      }).pipe(timeout(5000)),
    );
    if (decrypted !== expected) throw new Error('decrypted value mismatch');
  } finally {
    await broker.close();
  }

  process.stdout.write('node-read-compatible\n');
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exit(1);
});
