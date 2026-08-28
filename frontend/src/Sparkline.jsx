import { useState } from "react";

const WIDTH = 400;
const HEIGHT = 60;
const PADDING = 4;

const fmtPct = (v) => (Number.isInteger(v) ? v : Math.round(v * 10) / 10);

export default function Sparkline({ readings }) {
  const [hoverIdx, setHoverIdx] = useState(null);

  const points = readings
    .filter((r) => r.percent_full != null)
    .slice()
    .reverse(); // API returns newest-first; chart reads left-to-right oldest-first

  if (points.length < 2) return null;

  const values = points.map((p) => p.percent_full);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const coords = points.map((p, i) => {
    const x = PADDING + (i / (points.length - 1)) * (WIDTH - PADDING * 2);
    const y = HEIGHT - PADDING - ((p.percent_full - min) / range) * (HEIGHT - PADDING * 2);
    return { x, y };
  });

  const polyPoints = coords.map((c) => `${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(" ");

  const dateLabel = (ts) => new Date(ts).toLocaleDateString(undefined, { month: "short", day: "numeric" });
  const oldestLabel = dateLabel(points[0].ts);
  const newestLabel = dateLabel(points[points.length - 1].ts);

  function handleMove(e) {
    const rect = e.currentTarget.getBoundingClientRect();
    const frac = (e.clientX - rect.left) / rect.width;
    const idx = Math.round(frac * (points.length - 1));
    setHoverIdx(Math.max(0, Math.min(points.length - 1, idx)));
  }

  const hover = hoverIdx != null ? points[hoverIdx] : null;
  const hoverCoord = hoverIdx != null ? coords[hoverIdx] : null;
  const hoverPct = hoverCoord ? (hoverCoord.x / WIDTH) * 100 : 0;
  const tooltipTx = hoverPct < 20 ? "0%" : hoverPct > 80 ? "-100%" : "-50%";

  return (
    <div style={{ marginTop: "0.5rem" }}>
      <div
        style={{ position: "relative" }}
        onMouseMove={handleMove}
        onMouseLeave={() => setHoverIdx(null)}
        onClick={(e) => e.stopPropagation()} // don't collapse the card when scrubbing the chart
      >
        <svg
          width="100%"
          height={HEIGHT}
          viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
          preserveAspectRatio="none"
          style={{ display: "block" }}
        >
          <polyline points={polyPoints} fill="none" stroke="#C1502E" strokeWidth="2" />
          {hoverCoord && (
            <line x1={hoverCoord.x} y1="0" x2={hoverCoord.x} y2={HEIGHT} stroke="#5A4636" strokeOpacity="0.35" strokeWidth="1" />
          )}
        </svg>
        {hoverCoord && (
          <div
            style={{
              position: "absolute",
              left: `${hoverPct}%`,
              top: `${hoverCoord.y}px`,
              width: 8,
              height: 8,
              margin: "-4px 0 0 -4px",
              borderRadius: "50%",
              background: "#C1502E",
              pointerEvents: "none",
            }}
          />
        )}
        {hover && (
          <div
            style={{
              position: "absolute",
              left: `${hoverPct}%`,
              top: 0,
              transform: `translate(${tooltipTx}, calc(-100% - 6px))`,
              background: "#3A2A20",
              color: "#FFF6EC",
              fontSize: "0.7rem",
              lineHeight: 1.3,
              padding: "0.25rem 0.4rem",
              borderRadius: 4,
              whiteSpace: "nowrap",
              pointerEvents: "none",
            }}
          >
            {fmtPct(hover.percent_full)}% full
            <br />
            {new Date(hover.ts).toLocaleString(undefined, {
              month: "short",
              day: "numeric",
              hour: "numeric",
              minute: "2-digit",
            })}
          </div>
        )}
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: "0.75rem",
          color: "#5A4636",
        }}
      >
        <span>{oldestLabel}</span>
        <span>{newestLabel}</span>
      </div>
    </div>
  );
}
