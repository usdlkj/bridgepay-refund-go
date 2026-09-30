const fs = require('fs');
const path = require('path');

async function main() {
  const nodeRoot = process.env.NODE_REFUND_ROOT;
  const fixturePath = process.argv[2];
  const keyPath = process.argv[3];
  if (!nodeRoot || !fixturePath || !keyPath) throw new Error('missing parity inputs');

  require(path.join(nodeRoot, 'node_modules/reflect-metadata'));
  const { plainToInstance } = require(path.join(nodeRoot, 'node_modules/class-transformer'));
  const { validate } = require(path.join(nodeRoot, 'node_modules/class-validator'));
  const { Helper } = require(path.join(nodeRoot, 'dist/utils/helper.js'));
  const { maskAccountNumber } = require(path.join(nodeRoot, 'dist/utils/mask.util.js'));
  const { RefundService } = require(path.join(nodeRoot, 'dist/refund/refund.service.js'));
  const { CreateRefundDto } = require(path.join(nodeRoot, 'dist/refund/dto/create-refund.dto.js'));

  const fixture = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
  const create = fixture.cases.find((item) => item.name === 'refund-create-success-pending');
  const invalid = fixture.cases.find((item) => item.name === 'account-check-validation-failure');
  if (!create || !invalid) throw new Error('required golden cases are missing');

  process.env.DISBURSEMENT_FEE_FIX = '2100';
  process.env.PPN_VALUE = '11';
  const helper = new Helper({
    get(name) {
      if (name === 'refund.keyFilePrivate') return keyPath;
      if (name === 'refund.disbursementFeeFix') return 2100;
      if (name === 'refund.ppnValue') return 11;
      return undefined;
    },
  });
  const request = create.request;
  const amountData = await helper.totalAmountDisbursement(request.reqData.invoice.refundAmount);
  const service = Object.create(RefundService.prototype);
  const payout = await service.generateXenditRefundPayload({
    refundId: request.reqData.invoice.orderId,
    refundData: request,
    refundBankData: {
      bankCode: `ID_${request.reqData.account.bankId}`,
      bankNumber: request.reqData.account.accountNo,
      bankName: request.reqData.account.name,
    },
    refundAmountData: amountData,
    retryAttempt: [],
    requestData: [],
  });
  const validation = await validate(plainToInstance(CreateRefundDto, invalid.request));
  const signedPayload = JSON.stringify({ invoice: create.expected.retData.invoice });

  process.stdout.write(JSON.stringify({
    amountData,
    maskedAccount: maskAccountNumber(request.reqData.account.accountNo),
    status: await helper.statusWording('pendingDisbursement'),
    payout,
    validationRejected: validation.length > 0,
    signature: await helper.sign(signedPayload),
  }));
}

main().catch((error) => {
  process.stderr.write(error.message);
  process.exit(1);
});
