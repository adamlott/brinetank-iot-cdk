const API_URL = import.meta.env.VITE_API_URL;

const MOCK_DEVICES = {
  devices: [
    { device: "sensor-garage", percent_full: 62, ts: new Date(Date.now() - 2 * 3600e3).toISOString() },
    { device: "sensor-basement", percent_full: 18, ts: new Date(Date.now() - 40 * 60e3).toISOString() },
  ],
};

const MOCK_ORDERS = {
  orders: [
    { orderId: "0af1", submittedAt: "2026-08-24T15:42:00Z", saltType: "pellets", bags: "5", frequency: "one-time", status: "delivered", address: "1358 N 3200 W", city: "Provo", zipcode: "84601" },
    { orderId: "9c2e", submittedAt: "2026-07-30T09:05:00Z", saltType: "crystals", bags: "2", frequency: "monthly", status: "shipped", address: "1358 N 3200 W", city: "Provo", zipcode: "84601" },
    { orderId: "f73b", saltType: "pellets", bags: "1", frequency: "one-time", status: "pending", address: "1358 N 3200 W", city: "Provo", zipcode: "84601" },
  ],
};

function mockHistory(sensorId) {
  const readings = [];
  for (let i = 0; i < 30; i++) {
    readings.push({
      ts: new Date(Date.now() - i * 6 * 3600e3).toISOString(),
      percent_full: 40 + 30 * Math.sin(i / 5) + Math.random() * 5,
    });
  }
  return { sensorId, readings };
}

async function request(path, idToken) {
  if (idToken === "PREVIEW_MODE") {
    if (path === "/devices") return MOCK_DEVICES;
    if (path === "/orders") return MOCK_ORDERS;
    const match = path.match(/^\/devices\/(.+)\/history$/);
    if (match) return mockHistory(decodeURIComponent(match[1]));
  }
  const res = await fetch(`${API_URL}${path}`, {
    headers: { Authorization: idToken },
  });
  if (!res.ok) {
    throw new Error(`${path} failed: ${res.status}`);
  }
  return res.json();
}

export function listDevices(idToken) {
  return request("/devices", idToken);
}

export function getDeviceHistory(idToken, sensorId) {
  return request(`/devices/${encodeURIComponent(sensorId)}/history`, idToken);
}

export function listOrders(idToken) {
  return request("/orders", idToken);
}
