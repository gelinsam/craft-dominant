import React, { useEffect, useMemo, useState } from 'react';

function money(value) {
  const number = Number(value || 0);
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(number);
}

function percent(value) {
  return `${Math.round(Number(value || 0) * 100)}%`;
}

function badgeClass(type) {
  switch (type) {
    case 'pace_recovery':
      return 'bg-rose-50 text-rose-700 border-rose-200';
    default:
      return 'bg-slate-50 text-slate-700 border-slate-200';
  }
}

function Stat({ label, value, helper }) {
  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</div>
      <div className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">{value}</div>
      {helper ? <div className="mt-1 text-sm text-slate-500">{helper}</div> : null}
    </div>
  );
}

function Evidence({ evidence }) {
  if (!evidence) return null;
  const rows = [
    ['Pace', `${Math.abs(Number(evidence.pace_delta_pct || 0)).toFixed(0)}% behind`],
    ['Ticket gap', Number(evidence.gap_tickets || 0).toLocaleString()],
    ['Avg ticket', money(evidence.avg_ticket_price)],
    ['Runway', `${Number(evidence.days_until || 0)} days`],
    ['Recovery assumption', percent(evidence.recoverable_share_assumption)],
  ];

  return (
    <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
      {rows.map(([label, value]) => (
        <div key={label} className="rounded-lg border border-slate-200 bg-white px-3 py-2">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">{label}</div>
          <div className="mt-0.5 text-sm font-semibold text-slate-800">{value}</div>
        </div>
      ))}
    </div>
  );
}

function OpportunityCard({ item, rank }) {
  const weighted = Number(item.confidence_weighted_value || item.expected_net_value * item.confidence || 0);

  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-semibold text-slate-400">#{rank}</span>
            <span className={`rounded-full border px-2.5 py-1 text-xs font-semibold ${badgeClass(item.opportunity_type)}`}>
              {String(item.opportunity_type || 'opportunity').replaceAll('_', ' ').toUpperCase()}
            </span>
            <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-600">
              URGENCY {item.urgency}/10
            </span>
            {item.opportunity_id ? (
              <span className="font-mono text-[10px] text-slate-400">{item.opportunity_id}</span>
            ) : null}
          </div>

          <h2 className="mt-4 text-2xl font-semibold tracking-tight text-slate-950">{item.event_name}</h2>
          <h3 className="mt-1 text-lg font-medium text-slate-700">{item.title}</h3>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-600">{item.rationale}</p>

          <div className="mt-4 rounded-xl bg-slate-50 p-4">
            <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Recommended action</div>
            <div className="mt-1 text-sm font-medium text-slate-800">{item.recommended_action}</div>
            <Evidence evidence={item.evidence} />
          </div>
        </div>

        <div className="grid min-w-[260px] grid-cols-2 gap-3 lg:w-[340px]">
          <div className="rounded-xl bg-rose-50 p-4">
            <div className="text-xs font-semibold uppercase tracking-wide text-rose-700">Revenue at risk</div>
            <div className="mt-1 text-xl font-semibold text-rose-950">{money(item.revenue_at_risk)}</div>
          </div>
          <div className="rounded-xl bg-emerald-50 p-4">
            <div className="text-xs font-semibold uppercase tracking-wide text-emerald-700">Modeled recovery</div>
            <div className="mt-1 text-xl font-semibold text-emerald-950">{money(item.expected_revenue)}</div>
          </div>
          <div className="rounded-xl bg-indigo-50 p-4">
            <div className="text-xs font-semibold uppercase tracking-wide text-indigo-700">Weighted value</div>
            <div className="mt-1 text-xl font-semibold text-indigo-950">{money(weighted)}</div>
          </div>
          <div className="rounded-xl bg-slate-50 p-4">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Confidence</div>
            <div className="mt-1 text-xl font-semibold text-slate-950">{percent(item.confidence)}</div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function CommandPage() {
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);

  async function load() {
    setLoading(true);
    setError('');
    try {
      const response = await fetch('/api/command', { cache: 'no-store' });
      const body = await response.json();
      if (!response.ok) {
        throw new Error(body?.message || body?.error || `Command API returned ${response.status}`);
      }
      setData(body);
    } catch (err) {
      setError(err?.message || 'Command Center could not load.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  const opportunities = useMemo(() => data?.opportunities || [], [data]);

  return (
    <main className="min-h-screen bg-slate-50">
      <div className="mx-auto max-w-7xl px-6 py-8 lg:px-8 lg:py-12">
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <div className="text-xs font-bold uppercase tracking-[0.22em] text-indigo-600">Craft Dominant V2</div>
            <h1 className="mt-2 text-4xl font-semibold tracking-tight text-slate-950">Command Center</h1>
            <p className="mt-2 max-w-2xl text-slate-600">
              Ranked economic interventions across upcoming events. Revenue at risk is separated from modeled recoverable value so the system does not confuse exposure with expected lift.
            </p>
          </div>
          <button
            onClick={load}
            disabled={loading}
            className="rounded-xl bg-slate-950 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {loading ? 'Refreshing…' : 'Refresh intelligence'}
          </button>
        </div>

        {error ? (
          <div className="mt-8 rounded-2xl border border-rose-200 bg-rose-50 p-5 text-sm text-rose-800">
            <div className="font-semibold">Command Center unavailable</div>
            <div className="mt-1">{error}</div>
          </div>
        ) : null}

        <section className="mt-8 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <Stat label="Revenue at risk" value={money(data?.revenue_at_risk)} helper="Historical pacing gap exposure" />
          <Stat label="Modeled recovery" value={money(data?.gross_opportunity)} helper="Conservative recoverable portion" />
          <Stat label="Confidence-weighted" value={money(data?.confidence_weighted_net)} helper="Expected value after confidence" />
          <Stat label="Opportunities" value={data?.opportunity_count ?? '—'} helper="Material interventions surfaced" />
        </section>

        <section className="mt-8 space-y-4">
          {loading && !data ? (
            <div className="rounded-2xl border border-slate-200 bg-white p-10 text-center text-slate-500 shadow-sm">
              Building opportunity queue…
            </div>
          ) : null}

          {!loading && !error && opportunities.length === 0 ? (
            <div className="rounded-2xl border border-slate-200 bg-white p-10 text-center shadow-sm">
              <div className="text-lg font-semibold text-slate-900">No material interventions surfaced.</div>
              <div className="mt-1 text-sm text-slate-500">That can mean the portfolio is healthy, or that available data does not yet justify an intervention.</div>
            </div>
          ) : null}

          {opportunities.map((item, index) => (
            <OpportunityCard key={item.opportunity_id || `${item.event_id}-${item.opportunity_type}`} item={item} rank={index + 1} />
          ))}
        </section>
      </div>
    </main>
  );
}
