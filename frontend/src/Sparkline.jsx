const WIDTH = 400;
const HEIGHT = 60;
const PADDING = 4;

export default function Sparkline({ readings }) {
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
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  return (
    <svg
      width="100%"
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      preserveAspectRatio="none"
      style={{ marginTop: "0.5rem" }}
    >
      <polyline
        points={coords.join(" ")}
        fill="none"
        stroke="#1a73e8"
        strokeWidth="2"
      />
    </svg>
  );
}
