import { useEffect, useState } from "react";
import { signIn, signUp, confirmSignUp, signOut, getCurrentIdToken } from "./auth";
import { listDevices, getDeviceHistory } from "./api";
import Sparkline from "./Sparkline.jsx";

function AuthForm({ onSignedIn }) {
  const [mode, setMode] = useState("signin"); // signin | signup | confirm
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [code, setCode] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setError("");
    setBusy(true);
    try {
      if (mode === "signin") {
        const idToken = await signIn(email, password);
        onSignedIn(idToken);
      } else if (mode === "signup") {
        await signUp(email, password);
        setMode("confirm");
      } else if (mode === "confirm") {
        await confirmSignUp(email, code);
        setMode("signin");
      }
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <h1>Brine Tank Portal</h1>
      <div className="card">
        <form onSubmit={handleSubmit}>
          <input
            type="email"
            placeholder="Email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
          />
          {mode !== "confirm" && (
            <input
              type="password"
              placeholder="Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          )}
          {mode === "confirm" && (
            <input
              type="text"
              placeholder="Verification code"
              value={code}
              onChange={(e) => setCode(e.target.value)}
              required
            />
          )}
          {error && <div className="error">{error}</div>}
          <button type="submit" disabled={busy}>
            {mode === "signin" && "Log in"}
            {mode === "signup" && "Sign up"}
            {mode === "confirm" && "Confirm"}
          </button>
        </form>
        {mode === "signin" && (
          <button className="link" onClick={() => setMode("signup")}>
            Need an account? Sign up
          </button>
        )}
        {mode === "signup" && (
          <button className="link" onClick={() => setMode("signin")}>
            Already have an account? Log in
          </button>
        )}
      </div>
    </div>
  );
}

function DeviceCard({ device, idToken }) {
  const sensorId = device.device;
  const [history, setHistory] = useState(null);
  const [open, setOpen] = useState(false);

  async function toggle() {
    if (!open && !history) {
      try {
        const data = await getDeviceHistory(idToken, sensorId);
        setHistory(data.readings);
      } catch {
        setHistory([]);
      }
    }
    setOpen(!open);
  }

  const pct = device.percent_full;
  const updated = device.ts ? new Date(device.ts).toLocaleString() : "no readings yet";

  return (
    <div className="card" onClick={toggle} style={{ cursor: "pointer" }}>
      <h2>{sensorId}</h2>
      <div className="fill-pct">{pct != null ? `${pct}% full` : "—"}</div>
      <div className="meta">Last updated: {updated}</div>
      {open && history && history.length > 0 && (
        <Sparkline readings={history} />
      )}
      {open && history && history.length === 0 && (
        <div className="meta">No recent history.</div>
      )}
    </div>
  );
}

function Dashboard({ idToken, onSignOut }) {
  const [devices, setDevices] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    listDevices(idToken)
      .then((data) => setDevices(data.devices))
      .catch((err) => setError(err.message || String(err)));
  }, [idToken]);

  return (
    <div className="page">
      <h1>Brine Tank Portal</h1>
      {error && <div className="error">{error}</div>}
      {devices === null && !error && <div className="meta">Loading...</div>}
      {devices && devices.length === 0 && (
        <div className="meta">No devices linked to your account yet.</div>
      )}
      {devices &&
        devices.map((d) => (
          <DeviceCard key={d.device} device={d} idToken={idToken} />
        ))}
      <button className="signout" onClick={onSignOut}>
        Log out
      </button>
    </div>
  );
}

export default function App() {
  const [idToken, setIdToken] = useState(null);
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    // ?preview=1 renders the dashboard with mock data, no live Cognito/API needed —
    // handy for iterating on the UI before the backend is deployed.
    if (new URLSearchParams(window.location.search).get("preview") === "1") {
      setIdToken("PREVIEW_MODE");
      setChecked(true);
      return;
    }
    getCurrentIdToken()
      .then(setIdToken)
      .finally(() => setChecked(true));
  }, []);

  function handleSignOut() {
    signOut();
    setIdToken(null);
  }

  if (!checked) return null;

  return idToken ? (
    <Dashboard idToken={idToken} onSignOut={handleSignOut} />
  ) : (
    <AuthForm onSignedIn={setIdToken} />
  );
}
