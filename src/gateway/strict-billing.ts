import type { GatewayConfig } from '../types';

export const strictBillingLiveStreamingUnsupportedMessage =
  'Live streaming responses are not supported when strict billing enforcement is enabled.';

export function shouldAwaitBillingDelivery(
  config: Pick<GatewayConfig, 'billing'>
): boolean {
  const billing = config.billing;
  if (!billing?.enabled) {
    return false;
  }

  return Boolean(
    billing.delivery?.mode === 'await' ||
      billing.delivery?.requirePublisher ||
      billing.delivery?.requireOutbox
  );
}

export function shouldBlockLiveStreamingForStrictBilling(
  config: Pick<GatewayConfig, 'billing'>
): boolean {
  const billing = config.billing;
  if (!billing?.enabled) {
    return false;
  }

  return Boolean(
    billing.requireUsage ||
      billing.requireRates ||
      shouldAwaitBillingDelivery(config)
  );
}
