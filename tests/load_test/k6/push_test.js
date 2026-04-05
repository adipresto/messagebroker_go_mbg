import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';

const client = new grpc.Client();
client.load(['../../../proto/proto'], 'broker.proto');

export const options = {
  vus: 10,
  duration: '30s',
};

export default () => {
  if (__ITER == 0) {
    client.connect('localhost:50051', {
      plaintext: true
    });
  }

  const data = {
    id: `K6-MSG-${__VU}-${__ITER}`,
    payload: JSON.stringify({
      message: "Load test from k6",
      timestamp: Date.now(),
      index: __ITER
    }),
    headers: JSON.stringify({
      "X-Load-Test": "true",
      "X-Source": "k6"
    })
  };

  const response = client.invoke('broker.BrokerService/Push', data);

  check(response, {
    'status is OK': (r) => r && r.status === grpc.StatusOK,
    'success is true': (r) => r && r.message && r.message.success === true,
  });

  // Optional: small sleep to control rate if needed, or leave for max RPS
  // sleep(0.1);
};

export function teardown() {
  client.close();
}
