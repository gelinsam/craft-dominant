import React, { useEffect, useMemo, useState, useCallback } from 'react';

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

function statusBadge(status) {
  switch (status) {
    case 'new':
      return 'bg-slate-100 text-slate-600';
    case 'investigated':
      return 'bg-blue-50 text-blue-700';
    case 'proposed':
      return 'bg-amber-50 text-amber-700';
    case 'approved':
      return 'bg-emerald-50 text-emerald-700';
    case 'executing':
      return 'bg-indigo-50 text-indigo-700';
    case 'measuring':
      return 'bg-purple-50 text-purple-700';
    case 'learned':
      return 'bg-teal-50 text-teal-700';
    case 'rejected':
      return 'bg-rose-50 text-rose-700';
    case 'cancelled':
      return 'bg-slate-50 text-slate-400';
    default:
      return 'bg-slate-100 text-slate-600';
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

function RootCauseList({ causes }) {
  if (!causes || causes.length === 0) return null;
  return (
    <div className="mt-4 space-y-2">
      <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Root causes</div>
      {causes.map((rc, i) => (
        <div key={i} className="rounded-lg border border-slate-200 bg-white p-3">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-slate-800">{rc.cause}</span>
            <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-500">
              {Math.round(rc.confidence * 100)}% confidence
            </span>
          </div>
          {rc.evidence && rc.evidence.length > 0 ? (
            <ul className="mt-1.5 space-y-0.5">
              {rc.evidence.map((e, j) => (
                <li key={j} className="text-xs text-slate-500 pl-2 border-l-2 border-slate-200">{e}</li>
              ))}
            </ul>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function DiagnosisSummary({ diagnosis }) {
  if (!diagnosis) return null;
  const d = diagnosis;
  return (
    <div className="mt-4 rounded-xl border border-indigo-100 bg-indigo-50/50 p-4">
      <div className="text-xs font-semibold uppercase tracking-[0.16em] text-indigo-600">Diagnosis</div>

      <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-lg bg-white px-3 py-2 border border-indigo-100">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">Sell-through</div>
          <div className="mt-0.5 text-sm font-semibold text-slate-800">{d.sell_through_pct}%</div>
        </div>
        <div className="rounded-lg bg-white px-3 py-2 border border-indigo-100">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">Recent velocity</div>
          <div className="mt-0.5 text-sm font-semibold text-slate-800">
            {d.recent_velocity != null ? `${d.recent_velocity} tix/day` : 'N/A'}
          </div>
        </div>
        <div className="rounded-lg bg-white px-3 py-2 border border-indigo-100">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">Meta spend</div>
          <div className="mt-0.5 text-sm font-semibold text-slate-800">{money(d.meta_spend_total)}</div>
        </div>
        <div className="rounded-lg bg-white px-3 py-2 border border-indigo-100">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">CRM audience</div>
          <div className="mt-0.5 text-sm font-semibold text-slate-800">{(d.crm_audience_total || 0).toLocaleString()}</div>
        </div>
      </div>

      <RootCauseList causes={d.root_causes} />

      {d.recommended_intervention ? (
        <div className="mt-3 rounded-lg bg-emerald-50 border border-emerald-200 p-3">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-emerald-600">Recommended intervention</div>
          <div className="mt-1 text-sm font-medium text-emerald-900">
            {String(d.recommended_intervention).replaceAll('_', ' ').toUpperCase()}
          </div>
          <div className="mt-1 text-xs text-emerald-700">{d.recommendation_rationale}</div>
        </div>
      ) : null}

      {d.missing_data && d.missing_data.length > 0 ? (
        <div className="mt-3 rounded-lg bg-amber-50 border border-amber-200 p-3">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-amber-600">Missing data</div>
          <ul className="mt-1 space-y-0.5">
            {d.missing_data.map((m, i) => (
              <li key={i} className="text-xs text-amber-700">{m}</li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

function InterventionBadge({ intervention }) {
  if (!intervention) return null;
  return (
    <div className="mt-4 rounded-xl border border-slate-200 bg-white p-4">
      <div className="flex items-center gap-2">
        <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Intervention</div>
        <span className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${statusBadge(intervention.status)}`}>
          {String(intervention.status).toUpperCase()}
        </span>
      </div>
      <div className="mt-2 text-sm text-slate-700">{intervention.rationale}</div>
      <div className="mt-2 grid gap-2 sm:grid-cols-3">
        <div>
          <div className="text-[10px] font-semibold uppercase text-slate-400">Expected revenue</div>
          <div className="text-sm font-semibold text-slate-800">{money(intervention.expected_revenue)}</div>
        </div>
        <div>
          <div className="text-[10px] font-semibold uppercase text-slate-400">Expected cost</div>
          <div className="text-sm font-semibold text-slate-800">{money(intervention.expected_cost)}</div>
        </div>
        <div>
          <div className="text-[10px] font-semibold uppercase text-slate-400">Net value</div>
          <div className="text-sm font-semibold text-emerald-700">{money(intervention.expected_net_value)}</div>
        </div>
      </div>
      {intervention.campaign_draft_id ? (
        <div className="mt-2 text-xs text-slate-500">
          Campaign draft: <span className="font-mono">{intervention.campaign_draft_id}</span>
        </div>
      ) : null}
    </div>
  );
}

function OpportunityCard({ item, rank, onRefresh }) {
  const [diagnosis, setDiagnosis] = useState(null);
  const [diagnosisLoading, setDiagnosisLoading] = useState(false);
  const [diagnosisError, setDiagnosisError] = useState('');
  const [showDiagnosis, setShowDiagnosis] = useState(false);

  const [intervention, setIntervention] = useState(null);
  const [prepareLoading, setPrepareLoading] = useState(false);
  const [prepareError, setPrepareError] = useState('');

  const weighted = Number(item.confidence_weighted_value || item.expected_net_value * item.confidence || 0);

  const loadDiagnosis = useCallback(async () => {
    if (diagnosis) {
      setShowDiagnosis(!showDiagnosis);
      return;
    }
    setDiagnosisLoading(true);
    setDiagnosisError('');
    try {
      const res = await fetch(`/api/v2/opportunities/${item.event_id}/diagnosis`, {
        cache: 'no-store',
      });
      const body = await res.json();
      if (!res.ok) throw new Error(body?.message || body?.error || `Diagnosis failed: ${res.status}`);
      setDiagnosis(body.diagnosis);
      setShowDiagnosis(true);
    } catch (err) {
      setDiagnosisError(err?.message || 'Diagnosis failed');
    } finally {
      setDiagnosisLoading(false);
    }
  }, [diagnosis, showDiagnosis, item.event_id]);

  const prepare = useCallback(async () => {
    setPrepareLoading(true);
    setPrepareError('');
    try {
      const res = await fetch(`/api/v2/opportunities/${item.opportunity_id}/prepare`, {
        method: 'POST',
      });
      const body = await res.json();
      if (res.status === 409) {
        setIntervention(body.intervention);
        return;
      }
      if (!res.ok) throw new Error(body?.message || body?.error || `Prepare failed: ${res.status}`);
      setIntervention(body.intervention);
      if (!diagnosis && body.diagnosis_summary) {
        setDiagnosis(body.diagnosis_summary);
        setShowDiagnosis(true);
      }
    } catch (err) {
      setPrepareError(err?.message || 'Prepare failed');
    } finally {
      setPrepareLoading(false);
    }
  }, [item.opportunity_id, diagnosis]);

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

          {/* V2 actions — all calls go through server-side proxy */}
          <div className="mt-4 flex flex-wrap gap-2">
            <button
              onClick={loadDiagnosis}
              disabled={diagnosisLoading}
              className="rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-1.5 text-xs font-semibold text-indigo-700 transition hover:bg-indigo-100 disabled:opacity-50"
            >
              {diagnosisLoading ? 'Diagnosing…' : showDiagnosis ? 'Hide diagnosis' : 'Diagnose'}
            </button>
            <button
              onClick={prepare}
              disabled={prepareLoading || !!intervention}
              className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-700 transition hover:bg-emerald-100 disabled:opacity-50"
            >
              {prepareLoading ? 'Preparing…' : intervention ? 'Action prepared' : 'Prepare action'}
            </button>
          </div>

          {diagnosisError ? (
            <div className="mt-2 text-xs text-rose-600">{diagnosisError}</div>
          ) : null}
          {prepareError ? (
            <div className="mt-2 text-xs text-rose-600">{prepareError}</div>
          ) : null}

          {showDiagnosis ? <DiagnosisSummary diagnosis={diagnosis} /> : null}
          {intervention ? <InterventionBadge intervention={intervention} /> : null}
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
  const [interventions, setInterventions] = useState([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const [commandRes, interventionsRes] = await Promise.all([
        fetch('/api/command', { cache: 'no-store' }),
        fetch('/api/v2/interventions', { cache: 'no-store' }).catch(() => null),
      ]);

      const commandBody = await commandRes.json();
      if (!commandRes.ok) {
        throw new Error(commandBody?.message || commandBody?.error || `Command API returned ${commandRes.status}`);
      }
      setData(commandBody);

      if (interventionsRes && interventionsRes.ok) {
        const intBody = await interventionsRes.json();
        setInterventions(intBody.interventions || []);
      }
    } catch (err) {
      setError(err?.message || 'Command Center could not load.');
    } finally {
      setLoading(false);
    }
  }, []);

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

        {/* Active interventions summary */}
        {interventions.length > 0 ? (
          <section className="mt-6">
            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Active interventions</div>
            <div className="mt-2 flex flex-wrap gap-2">
              {interventions.map((intv) => (
                <div key={intv.id} className="flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm">
                  <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${statusBadge(intv.status)}`}>
                    {String(intv.status).toUpperCase()}
                  </span>
                  <span className="text-slate-700">{String(intv.intervention_type).replaceAll('_', ' ')}</span>
                  <span className="font-mono text-[10px] text-slate-400">{intv.id?.slice(0, 8)}</span>
                </div>
              ))}
            </div>
          </section>
        ) : null}

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
            <OpportunityCard
              key={item.opportunity_id || `${item.event_id}-${item.opportunity_type}`}
              item={item}
              rank={index + 1}
              onRefresh={load}
            />
          ))}
        </section>
      </div>
    </main>
  );
}
