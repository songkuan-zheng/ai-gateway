import { describe, expect, it } from 'vitest';
import { parseGatewayConfigFromRaw } from '../config';
import {
  shouldAwaitBillingDelivery,
  shouldBlockLiveStreamingForStrictBilling
} from './strict-billing';

describe('strict billing delivery', () => {
  it('awaits and blocks live streaming when a durable outbox is required in async mode', () => {
    const config = parseGatewayConfigFromRaw({
      billing: {
        enabled: true,
        delivery: {
          mode: 'async',
          requireOutbox: true
        }
      }
    });

    expect(shouldAwaitBillingDelivery(config)).toBe(true);
    expect(shouldBlockLiveStreamingForStrictBilling(config)).toBe(true);
  });

  it('keeps optional async delivery non-blocking', () => {
    const config = parseGatewayConfigFromRaw({
      billing: {
        enabled: true,
        delivery: {
          mode: 'async',
          requirePublisher: false,
          requireOutbox: false
        }
      }
    });

    expect(shouldAwaitBillingDelivery(config)).toBe(false);
    expect(shouldBlockLiveStreamingForStrictBilling(config)).toBe(false);
  });
});
