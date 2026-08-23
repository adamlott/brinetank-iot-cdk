const API_URL = import.meta.env.VITE_API_URL;

const MOCK_DEVICES = {
  devices: [
    { device: "sensor-garage", percent_full: 62, ts: new Date(Date.now() - 2 * 3600e3).toISOString() },
    { device: "sensor-basement", percent_full: 18, ts: new Date(Date.now() - 40 * 60e3).toISOString() },
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
