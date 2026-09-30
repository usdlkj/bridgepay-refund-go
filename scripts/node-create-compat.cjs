'use strict';

const path = require('node:path');
const { createRequire } = require('node:module');

async function main() {
  const root = process.env.NODE_REFUND_ROOT;
  const encryptorRoot = process.env.NODE_ENCRYPTOR_ROOT;
  const id = process.env.COMPAT_BANK_DATA_ID;
  const value = process.env.COMPAT_EXPECTED_VALUE;
  if (!root || !encryptorRoot || !id || !value) throw new Error('missing compatibility test configuration');

  const nodeRequire = createRequire(path.join(root, 'package.json'));
  const { Client } = nodeRequire('pg');
  const encryptorRequire = createRequire(path.join(encryptorRoot, 'package.json'));
  const { ClientProxyFactory, Transport } = encryptorRequire('@nestjs/microservices');
  const { firstValueFrom, timeout } = encryptorRequire('rxjs');
  const broker = ClientProxyFactory.create({
    transport: Transport.RMQ,
    options: {
      urls: [process.env.RABBITMQ_URL],
      queue: process.env.RABBITMQ_ENCRYPTOR_QUEUE,
      queueOptions: { durable: true },
    },
  });
  const context = 'refund.bankData.accountNumber';
  let ciphertext;
  let blindIndex;
  try {
    ciphertext = await firstValueFrom(
      broker.send('encrypt', {
        value: Buffer.from(value).toString('base64'),
        aad: Buffer.from(context).toString('base64'),
        context,
      }).pipe(timeout(5000)),
    );
    blindIndex = await firstValueFrom(
      broker.send('blind-index', { value, context }).pipe(timeout(5000)),
    );
  } finally {
    await broker.close();
  }

  const database = new Client({ connectionString: process.env.DATABASE_URL });
  await database.connect();
  try {
    await database.query(
      `INSERT INTO bank_datas
         (id, bank_code, account_number_enc, account_number_hash,
          account_status, account_result, last_check_at)
       VALUES ($1, 'SYNTHETIC_NODE', $2, $3, 'pending', 'pending', now())`,
      [id, ciphertext, blindIndex],
    );
  } finally {
    await database.end();
  }
  process.stdout.write('node-create-compatible\n');
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exit(1);
});
