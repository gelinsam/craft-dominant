import React, { useState, useEffect, useCallback } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

const API_BASE = 'https://craft-dominant-production.up.railway.app';

const DECISIONS = {
  pivot: { color: '#dc2626', bg: '#fef2f2', icon: '\u{1F6A8}', label: 'PIVOT' },
  push: { color: '#ea580c', bg: '#fff7ed', icon: '\u{1F4A5}', label: 'PUSH' },
  maintain: { color: '#16a34a', bg: '#f0fdf4', icon: '\u2705', label: 'MAINTAIN' },
  coast: { color: '#2563eb', bg: '#eff6ff', icon: '\u{1F30A}', label: 'COAST' },
  not_started: { color: '#6b7280', bg: '#f3f4f6', icon: '\u23F3', label: 'FUTURE' },
};

const SEGMENTS = {
  champion: { color: '#16a34a', label: 'Champions' },
  loyal: { color: '#2563eb', label: 'Loyal' },
  potential: { color: '#8b5cf6', label: 'Potential' },
  at_risk: { color: '#ea580c', label: 'At Risk' },
  hibernating: { color: '#dc2626', label: 'Hibernating' },
  other: { color: '#6b7280', label: 'Other' },
};

const AUDIENCE_COLORS = {
  past_attendees: { color: '#7c3aed', bg: '#f5f3ff', border: '#ddd6fe' },
  city_prospects: { color: '#0891b2', bg: '#ecfeff', border: '#a5f3fc' },
  type_fans: { color: '#db2777', bg: '#fdf2f8', border: '#fbcfe8' },
  at_risk: { color: '#ea580c', bg: '#fff7ed', border: '#fed7aa' },
};

const AUDIENCE_ICONS = {
  past_attendees: '\u{1F3AF}',
  city_prospects: '\u{1F3D9}\uFE0F',
  type_fans: '\u2764\uFE0F',
  at_risk: '\u26A0\uFE0F',
};

const URGENCY_COLORS = {
  critical: { bg: '#fef2f2', color: '#dc2626', label: 'CRITICAL' },
  now: { bg: '#fff7ed', color: '#ea580c', label: 'DO NOW' },
  soon: { bg: '#fffbeb', color: '#d97706', label: 'THIS WEEK' },
};

const Card = ({ children, className = '', onClick }) => (
  <div className={`bg-white rounded-xl shadow-sm border border-gray-100 ${className} ${onClick ? 'cursor-pointer hover:shadow-md transition-shadow' : ''}`} onClick={onClick}>
    {children}
  </div>
);

const Badge = ({ children, color, bg }) => (
  <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-semibold" style={{ backgroundColor: bg, color }}>
    {children}
  </span>
);

const Stat = ({ label, value }) => (
  <div>
    <div className="text-sm text-gray-500">{label}</div>
    <div className="text-2xl font-bold text-gray-900">{value}</div>
  </div>
);

const EventCard = ({ event, selected, onSelect }) => {
  const cfg = DECISIONS[event.decision] || DECISIONS.maintain;
  return (
    <Card className={`p-4 ${selected ? 'ring-2 ring-blue-500' : ''}`} onClick={() => onSelect(event)}>
      <div className="flex justify-between items-start mb-3">
        <div>
          <div className="font-bold text-gray-900 text-sm">{event.event_name}</div>
          <div className="text-xs text-gray-500">{event.days_until}d</div>
        </div>
        <Badge color={cfg.color} bg={cfg.bg}>{cfg.icon} {cfg.label}</Badge>
      </div>
      <div className="mb-2">
        <div className="flex justify-between text-xs mb-1">
          <span>{event.tickets_sold?.toLocaleString()}/{event.capacity?.toLocaleString()}</span>
          <span className="font-semibold">{event.sell_through?.toFixed(0)}%</span>
        </div>
        <div className="h-2 bg-gray-100 rounded-full"><div className="h-full rounded-full" style={{ width: `${Math.min(100, event.sell_through || 0)}%`, backgroundColor: cfg.color }} /></div>
      </div>
      {event.pace_vs_historical !== 0 && (
        <div className={`text-xs ${event.pace_vs_historical >= 0 ? 'text-green-600' : 'text-red-600'}`}>
          {event.pace_vs_historical >= 0 ? '+' : ''}{event.pace_vs_historical?.toFixed(0)}% vs historical
        </div>
      )}
    </Card>
  );
};

const EventDetail = ({ event }) => {
  if (!event) return <Card className="p-8 text-center text-gray-500">Select an event</Card>;
  const cfg = DECISIONS[event.decision] || DECISIONS.maintain;
  return (
    <Card className="p-5">
      <div className="flex justify-between mb-4">
        <div>
          <h2 className="text-xl font-bold">{event.event_name}</h2>
          <p className="text-gray-500">{event.event_date?.slice(0, 10)} &bull; {event.days_until} days</p>
        </div>
        <Badge color={cfg.color} bg={cfg.bg}>{cfg.icon} {cfg.label}</Badge>
      </div>
      <div className="grid grid-cols-4 gap-4 mb-6">
        {[
          { l: 'Tickets', v: event.tickets_sold?.toLocaleString() },
          { l: 'Revenue', v: `$${event.revenue?.toLocaleString()}` },
          { l: 'Spend', v: `$${event.ad_spend?.toLocaleString()}` },
          { l: 'CAC', v: `$${event.cac?.toFixed(2)}` }
        ].map((s, i) => (
          <div key={i} className="text-center p-3 bg-gray-50 rounded-lg">
            <div className="text-xl font-bold">{s.v}</div>
            <div className="text-xs text-gray-500">{s.l}</div>
          </div>
        ))}
      </div>
      {(event.historical_comparisons?.length > 0 || event.historical_median_at_point > 0) && (
        <div className="mb-6 p-4 bg-gray-50 rounded-lg">
          <h3 className="font-semibold mb-3">Historical Pacing ({event.days_until}d out)</h3>
          {event.event_name?.includes(' - ') && <p className="text-xs text-gray-400 mt-1">Comparing {event.event_name.split(' - ').pop()}s across years</p>}
          <table className="w-full text-sm">
            <thead><tr className="border-b text-gray-500">
              <th className="text-left py-2">Year</th>
              <th className="text-right py-2">@ {event.days_until}d out</th>
              <th className="text-right py-2">Spend @ {event.days_until}d</th>
              <th className="text-right py-2">Final</th>
              <th className="text-right py-2">Capacity</th>
            </tr></thead>
            <tbody>
              {event.historical_comparisons?.map((h, i) => {
                const currSpend = h.at_days_out?.ad_spend || 0;
                const prevH = i > 0 ? event.historical_comparisons[i-1] : null;
                const prevSpend = prevH?.at_days_out?.ad_spend || 0;
                const spendDelta = prevSpend > 0 ? ((currSpend - prevSpend) / prevSpend * 100) : null;
                return (
                <tr key={i} className="border-b border-gray-100">
                  <td className="py-2"><div className="font-medium">{h.year}</div><div className="text-xs text-gray-400">{h.event_date?.slice(0, 10)}</div></td>
                  <td className="text-right py-2 font-medium">{h.at_days_out ? `${h.at_days_out.tickets.toLocaleString()} (${h.at_days_out.sell_through?.toFixed(1)}%)` : '\u2014'}</td>
                  <td className="text-right py-2">
                    {currSpend > 0 ? <span className="font-medium">${currSpend.toLocaleString()}</span> : '\u2014'}
                    {spendDelta !== null && <div className={`text-xs ${spendDelta >= 0 ? 'text-red-500' : 'text-green-500'}`}>{spendDelta >= 0 ? '+' : ''}{spendDelta.toFixed(0)}% YOY</div>}
                  </td>
                  <td className="text-right py-2">{h.final_tickets?.toLocaleString()} ({h.final_sell_through}%)</td>
                  <td className="text-right py-2 text-gray-500">{h.capacity?.toLocaleString()}</td>
                </tr>);
              })}
              <tr className="font-bold bg-blue-50">
                <td className="py-2 rounded-l"><div>{new Date(event.event_date).getFullYear()} <span className="text-xs font-normal text-gray-500">current</span></div><div className="text-xs text-gray-400 font-normal">{event.event_date?.slice(0, 10)}</div></td>
                <td className="text-right py-2 text-blue-600">{event.tickets_sold?.toLocaleString()} ({event.sell_through?.toFixed(1)}%)</td>
                <td className="text-right py-2 text-blue-600">
                  {event.ad_spend > 0 ? (<div><span>${event.ad_spend?.toLocaleString()}</span>
                    {(() => { const lc = event.historical_comparisons?.[event.historical_comparisons.length - 1]; const ls = lc?.at_days_out?.ad_spend || 0; if (ls > 0 && event.ad_spend > 0) { const d = ((event.ad_spend - ls) / ls * 100); return <div className={`text-xs font-normal ${d >= 0 ? 'text-red-500' : 'text-green-500'}`}>{d >= 0 ? '+' : ''}{d.toFixed(0)}% YOY</div>; } return null; })()}
                  </div>) : '$0'}
                </td>
                <td className="text-right py-2 text-blue-600">{event.projected_final?.toLocaleString()} proj</td>
                <td className="text-right py-2 text-gray-500 rounded-r">{event.capacity?.toLocaleString()}</td>
              </tr>
            </tbody>
          </table>
          {event.pace_vs_historical !== 0 && event.historical_median_at_point > 0 && (
            <div className="mt-3 text-sm">Pace vs median: <strong className={event.pace_vs_historical >= 0 ? 'text-green-600' : 'text-red-600'}>{event.pace_vs_historical >= 0 ? '+' : ''}{event.pace_vs_historical?.toFixed(0)}%</strong></div>
          )}
        </div>
      )}
      <div className="mb-6 p-4 bg-blue-50 rounded-lg">
        <h3 className="font-semibold mb-1">Projection</h3>
        <div className="text-3xl font-bold text-blue-600">{event.projected_final?.toLocaleString()}</div>
        <div className="text-sm text-gray-600">Range: {event.projected_range?.[0]?.toLocaleString()} - {event.projected_range?.[1]?.toLocaleString()}</div>
      </div>
      <div className="p-4 rounded-lg" style={{ backgroundColor: cfg.bg }}>
        <h3 className="font-semibold mb-2" style={{ color: cfg.color }}>Actions</h3>
        <p className="text-sm mb-3">{event.rationale}</p>
        <ul className="space-y-2">
          {event.actions?.map((a, i) => (
            <li key={i} className="flex items-start gap-2 text-sm">
              <span className="w-5 h-5 rounded-full flex items-center justify-center text-xs text-white font-bold shrink-0" style={{ backgroundColor: cfg.color }}>{i + 1}</span>
              {a}
            </li>
          ))}
        </ul>
      </div>
    </Card>
  );
};

const SortHeader = ({ label, field, sortBy, sortOrder, onSort, align }) => {
  const active = sortBy === field;
  return (
    <th className={`px-4 py-3 cursor-pointer hover:bg-gray-100 select-none ${align === 'right' ? 'text-right' : 'text-left'}`}
        onClick={() => onSort(field)}>
      <span className="inline-flex items-center gap-1">
        {label}
        {active ? (
          <span className="text-blue-600">{sortOrder === 'DESC' ? '\u25BC' : '\u25B2'}</span>
        ) : (
          <span className="text-gray-300">{'\u25BC'}</span>
        )}
      </span>
    </th>
  );
};

const CustomerTable = ({ customers, onSelect, sortBy, sortOrder, onSort }) => (
  <table className="w-full text-sm">
    <thead className="bg-gray-50 text-left font-semibold text-gray-600">
      <tr>
        <th className="px-4 py-3">Email</th>
        <SortHeader label="City" field="favorite_city" sortBy={sortBy} sortOrder={sortOrder} onSort={onSort} />
        <th className="px-4 py-3">Segment</th>
        <SortHeader label="Type" field="favorite_event_type" sortBy={sortBy} sortOrder={sortOrder} onSort={onSort} />
        <SortHeader label="Events" field="total_events" sortBy={sortBy} sortOrder={sortOrder} onSort={onSort} align="right" />
        <SortHeader label="Orders" field="total_orders" sortBy={sortBy} sortOrder={sortOrder} onSort={onSort} align="right" />
        <SortHeader label="Spent" field="total_spent" sortBy={sortBy} sortOrder={sortOrder} onSort={onSort} align="right" />
        <SortHeader label="LTV" field="ltv_score" sortBy={sortBy} sortOrder={sortOrder} onSort={onSort} align="right" />
      </tr>
    </thead>
    <tbody className="divide-y">
      {customers.map((c, i) => {
        const seg = SEGMENTS[c.rfm_segment] || SEGMENTS.other;
        return (
          <tr key={i} className="hover:bg-gray-50 cursor-pointer" onClick={() => onSelect(c.email)}>
            <td className="px-4 py-3 font-medium truncate max-w-[200px]" title={c.email}>{c.email}</td>
            <td className="px-4 py-3 text-gray-600">{c.favorite_city || '\u2014'}</td>
            <td className="px-4 py-3"><Badge color={seg.color} bg={seg.color + '20'}>{seg.label}</Badge></td>
            <td className="px-4 py-3 text-gray-600 capitalize">{c.favorite_event_type || '\u2014'}</td>
            <td className="px-4 py-3 text-right">{c.total_events}</td>
            <td className="px-4 py-3 text-right">{c.total_orders}</td>
            <td className="px-4 py-3 text-right">${c.total_spent?.toFixed(0)}</td>
            <td className="px-4 py-3 text-right font-bold text-blue-600">{c.ltv_score?.toFixed(0)}</td>
          </tr>
        );
      })}
    </tbody>
  </table>
);

const AudienceCard = ({ audienceKey, data, selected, onSelect, onExport }) => {
  const colors = AUDIENCE_COLORS[audienceKey] || AUDIENCE_COLORS.past_attendees;
  const icon = AUDIENCE_ICONS[audienceKey] || '\u{1F3AF}';
  const isActive = selected === audienceKey;
  return (
    <div
      className={`rounded-xl border-2 p-4 cursor-pointer transition-all ${isActive ? 'shadow-lg scale-[1.02]' : 'hover:shadow-md'}`}
      style={{ backgroundColor: colors.bg, borderColor: isActive ? colors.color : colors.border }}
      onClick={() => onSelect(isActive ? null : audienceKey)}
    >
      <div className="flex items-start justify-between mb-2">
        <span className="text-2xl">{icon}</span>
        {data.count > 0 && (
          <button onClick={(e) => { e.stopPropagation(); onExport(audienceKey); }}
            className="text-xs px-2 py-1 rounded-md font-medium transition-colors"
            style={{ backgroundColor: colors.color + '20', color: colors.color }}
            title="Export CSV for email campaign">
            Export CSV
          </button>
        )}
      </div>
      <div className="text-3xl font-bold mb-1" style={{ color: colors.color }}>{data.count.toLocaleString()}</div>
      <div className="font-semibold text-gray-900 text-sm mb-1">{data.label}</div>
      <div className="text-xs text-gray-500 mb-2 leading-relaxed">{data.description}</div>
      {data.historical_value > 0 && (
        <div className="text-xs font-medium" style={{ color: colors.color }}>
          ${data.historical_value.toLocaleString()} lifetime value
        </div>
      )}
      {/* Segment breakdown for past_attendees */}
      {audienceKey === 'past_attendees' && data.segment_breakdown && (
        <div className="mt-3 pt-3 border-t flex flex-wrap gap-1" style={{ borderColor: colors.border }}>
          {Object.entries(data.segment_breakdown).map(([seg, count]) => {
            const s = SEGMENTS[seg] || SEGMENTS.other;
            return <span key={seg} className="text-xs px-1.5 py-0.5 rounded" style={{ backgroundColor: s.color + '15', color: s.color }}>{count} {s.label}</span>;
          })}
        </div>
      )}
    </div>
  );
};

const CustomerModal = ({ customer, orders, onClose }) => {
  if (!customer) return null;
  const seg = SEGMENTS[customer.rfm_segment] || SEGMENTS.other;
  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-white rounded-xl max-w-2xl w-full max-h-[90vh] overflow-auto m-4" onClick={e => e.stopPropagation()}>
        <div className="p-5 border-b flex justify-between">
          <div>
            <h2 className="text-xl font-bold">{customer.email}</h2>
            <div className="flex gap-2 mt-2">
              <Badge color={seg.color} bg={seg.color + '20'}>{seg.label}</Badge>
              <Badge color="#6b7280" bg="#f3f4f6">{customer.timing_segment}</Badge>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-2xl">&times;</button>
        </div>
        <div className="grid grid-cols-4 border-b">
          {[
            { l: 'Orders', v: customer.total_orders },
            { l: 'Tickets', v: customer.total_tickets },
            { l: 'Spent', v: `$${customer.total_spent?.toFixed(0)}` },
            { l: 'LTV', v: customer.ltv_score?.toFixed(0) }
          ].map((s, i) => (
            <div key={i} className="text-center p-4"><div className="text-2xl font-bold">{s.v}</div><div className="text-sm text-gray-500">{s.l}</div></div>
          ))}
        </div>
        <div className="p-5">
          <div className="grid grid-cols-2 gap-4 mb-4">
            <div className="p-3 bg-gray-50 rounded-lg"><div className="text-xs text-gray-500">Favorite Type</div><div className="font-semibold">{customer.favorite_event_type || 'N/A'}</div></div>
            <div className="p-3 bg-gray-50 rounded-lg"><div className="text-xs text-gray-500">Favorite City</div><div className="font-semibold">{customer.favorite_city || 'N/A'}</div></div>
          </div>
          <h3 className="font-semibold mb-3">Order History ({orders?.length || 0})</h3>
          <div className="space-y-2 max-h-60 overflow-auto">
            {orders?.map((o, i) => (
              <div key={i} className="flex justify-between p-3 bg-gray-50 rounded-lg text-sm">
                <div><div className="font-medium">{o.event_name}</div><div className="text-gray-500">{o.order_timestamp?.slice(0, 10)}</div></div>
                <div className="text-right"><div className="font-medium">${o.gross_amount?.toFixed(2)}</div><div className="text-gray-500">{o.ticket_count} ticket(s)</div></div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default function CraftDashboard() {
  const [tab, setTab] = useState('events');
  const [dashboard, setDashboard] = useState(null);
  const [customers, setCustomers] = useState([]);
  const [customerTotal, setCustomerTotal] = useState(0);
  const [selectedEvent, setSelectedEvent] = useState(null);
  const [selectedCustomer, setSelectedCustomer] = useState(null);
  const [customerOrders, setCustomerOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [syncError, setSyncError] = useState(null);

  // CRM state
  const [crmView, setCrmView] = useState('targeting');
  const [targetEvent, setTargetEvent] = useState('');
  const [targeting, setTargeting] = useState(null);
  const [targetingLoading, setTargetingLoading] = useState(false);
  const [selectedAudience, setSelectedAudience] = useState(null);
  const [intel, setIntel] = useState(null);
  const [intelLoading, setIntelLoading] = useState(false);
  const [intelOpen, setIntelOpen] = useState({});

  // Overlap
  const [overlap, setOverlap] = useState(null);
  const [overlapLoading, setOverlapLoading] = useState(false);
  const [overlapCity, setOverlapCity] = useState('');
  const [overlapView, setOverlapView] = useState('matrix'); // matrix | pairs

  // CRM Browse
  const [crmSearch, setCrmSearch] = useState('');
  const [crmSegment, setCrmSegment] = useState('');
  const [crmCity, setCrmCity] = useState('');
  const [crmType, setCrmType] = useState('');
  const [crmSort, setCrmSort] = useState('ltv_score');
  const [crmOrder, setCrmOrder] = useState('DESC');
  const [crmPage, setCrmPage] = useState(0);
  const [crmCities, setCrmCities] = useState([]);
  const [crmTypes, setCrmTypes] = useState([]);
  const CRM_LIMIT = 50;

  const fetchDashboard = () => {
    fetch(`${API_BASE}/api/dashboard`).then(r => r.json()).then(d => {
      setDashboard(d);
      if (d.events?.length) setSelectedEvent(d.events[0]);
      setLoading(false);
      if (d.sync?.running) { setSyncing(true); setTimeout(fetchDashboard, 10000); }
      else { setSyncing(false); if (d.sync?.error) setSyncError(d.sync.error); }
    }).catch(() => setLoading(false));
  };
  useEffect(() => { fetchDashboard(); }, []);

  const fetchCustomers = () => {
    const params = new URLSearchParams({ limit: CRM_LIMIT, offset: crmPage * CRM_LIMIT, sort: crmSort, order: crmOrder });
    if (crmSearch) params.set('search', crmSearch);
    if (crmSegment) params.set('segment', crmSegment);
    if (crmCity) params.set('city', crmCity);
    if (crmType) params.set('event_type', crmType);
    fetch(`${API_BASE}/api/customers?${params}`).then(r => r.json()).then(d => { setCustomers(d.customers || []); setCustomerTotal(d.total || 0); });
  };

  useEffect(() => {
    if (tab === 'crm' && crmView === 'browse') {
      fetchCustomers();
      if (!crmCities.length) {
        fetch(`${API_BASE}/api/customers/cities`).then(r => r.json()).then(setCrmCities).catch(() => {});
        fetch(`${API_BASE}/api/customers/event-types`).then(r => r.json()).then(setCrmTypes).catch(() => {});
      }
    }
  }, [tab, crmView, crmSearch, crmSegment, crmCity, crmType, crmSort, crmOrder, crmPage]);

  const fetchTargeting = useCallback((eventId) => {
    if (!eventId) { setTargeting(null); return; }
    setTargetingLoading(true); setSelectedAudience(null);
    fetch(`${API_BASE}/api/targeting/${eventId}`).then(r => r.json()).then(d => { setTargeting(d); setTargetingLoading(false); }).catch(() => setTargetingLoading(false));
    setIntelLoading(true);
    fetch(`${API_BASE}/api/intelligence/${eventId}`).then(r => r.json()).then(d => { setIntel(d); setIntelLoading(false); }).catch(() => setIntelLoading(false));
  }, []);

  useEffect(() => {
    if (tab === 'overlap' && !overlap && !overlapLoading) {
      setOverlapLoading(true);
      fetch(`${API_BASE}/api/overlap`).then(r => r.json()).then(d => {
        setOverlap(d);
        if (d.cities?.length && !overlapCity) setOverlapCity(d.cities[0]);
        setOverlapLoading(false);
      }).catch(() => setOverlapLoading(false));
    }
  }, [tab]);

  useEffect(() => { if (tab === 'crm' && crmView === 'targeting' && targetEvent) fetchTargeting(targetEvent); }, [tab, crmView, targetEvent, fetchTargeting]);
  useEffect(() => { if (tab === 'crm' && crmView === 'targeting' && !targetEvent && dashboard?.events?.length) setTargetEvent(dashboard.events[0].event_id); }, [tab, crmView, dashboard]);

  const handleSelectCustomer = (email) => {
    fetch(`${API_BASE}/api/customers/${encodeURIComponent(email)}`).then(r => r.json()).then(d => { setSelectedCustomer(d.customer); setCustomerOrders(d.orders || []); });
  };
  const handleExportCSV = (audienceKey) => {
    if (!targetEvent) return;
    const keyParam = targeting?.export_token ? `&key=${encodeURIComponent(targeting.export_token)}` : '';
    window.open(`${API_BASE}/api/export/csv?event_id=${targetEvent}&audience=${audienceKey}${keyParam}`, '_blank');
  };
  const handleExportAll = () => {
    if (!targetEvent) return;
    const keyParam = targeting?.export_token ? `&key=${encodeURIComponent(targeting.export_token)}` : '';
    window.open(`${API_BASE}/api/export/csv?event_id=${targetEvent}&audience=all${keyParam}`, '_blank');
  };
  const handleIntelExport = (audienceKey) => {
    if (!targetEvent) return;
    const keyParam = intel?.export_token ? `&key=${encodeURIComponent(intel.export_token)}` : '';
    window.open(`${API_BASE}/api/export/intelligence-csv?event_id=${targetEvent}&audience=${audienceKey}${keyParam}`, '_blank');
  };
  const toggleIntel = (key) => setIntelOpen(prev => ({ ...prev, [key]: !prev[key] }));

  const triggerSync = () => {
    setSyncing(true); setSyncError(null);
    fetch(`${API_BASE}/api/sync`).then(r => r.json()).then(() => {
      const poll = () => { fetch(`${API_BASE}/api/sync-status`).then(r => r.json()).then(s => { if (s.done) { setSyncing(false); if (s.error) setSyncError(s.error); fetchDashboard(); } else setTimeout(poll, 5000); }); };
      setTimeout(poll, 5000);
    });
  };

  if (loading) return (<div className="min-h-screen bg-gray-50 flex flex-col items-center justify-center gap-4"><div className="text-2xl font-bold">Craft Dominant</div><div className="text-gray-500">Loading dashboard...</div></div>);

  const { portfolio, decisions, events, customers: cs } = dashboard || {};
  const isEmpty = !events?.length && !portfolio?.total_tickets;
  const audienceCustomers = targeting?.audiences?.[selectedAudience]?.customers || [];
  const audienceData = targeting?.audiences?.[selectedAudience];
  const totalTargetable = targeting ? Object.values(targeting.audiences || {}).reduce((sum, a) => sum + a.count, 0) : 0;
  const rg = targeting?.revenue_gap || {};
  const rb = targeting?.repeat_buyers || {};
  const qw = targeting?.quick_win || {};

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b sticky top-0 z-40">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div><h1 className="text-2xl font-bold">Craft Dominant</h1><p className="text-sm text-gray-500">{dashboard?.updated_at?.slice(0, 16)}</p></div>
          <div className="flex items-center gap-4">
            <div className="flex gap-2">
              {Object.entries(decisions || {}).map(([d, n]) => { const cfg = DECISIONS[d]; return cfg && n > 0 ? (<div key={d} className="flex items-center gap-1 px-3 py-1 rounded-full" style={{ backgroundColor: cfg.bg }}><span>{cfg.icon}</span><span className="font-bold" style={{ color: cfg.color }}>{n}</span></div>) : null; })}
            </div>
            <div className="flex bg-gray-100 rounded-lg p-1">
              {['events', 'crm', 'overlap'].map(t => (<button key={t} onClick={() => setTab(t)} className={`px-4 py-2 rounded-lg font-medium ${tab === t ? 'bg-white shadow' : 'text-gray-600'}`}>{t === 'events' ? 'Events' : t === 'crm' ? 'CRM' : 'Overlap'}</button>))}
            </div>
            <button onClick={() => { if (!syncing) triggerSync(); }} className={`px-3 py-2 rounded-lg text-sm font-medium ${syncing ? 'bg-gray-200 text-gray-400' : 'bg-blue-50 text-blue-600 hover:bg-blue-100'}`} disabled={syncing}>{syncing ? 'Syncing...' : 'Refresh Data'}</button>
          </div>
        </div>
      </header>

      {syncing && <div className="bg-blue-600 text-white py-2 px-6 text-center text-sm">Syncing data from Eventbrite... Dashboard will update automatically.</div>}
      {syncError && <div className="bg-red-100 text-red-700 py-2 px-6 text-center text-sm">Sync error: {syncError}</div>}

      <main className="max-w-7xl mx-auto px-6 py-6">
        {isEmpty && !syncing && (
          <Card className="p-8 text-center mb-6">
            <div className="text-xl font-bold text-gray-700 mb-2">No Data Yet</div>
            <p className="text-gray-500 mb-4">Sync with Eventbrite to load your events and sales data.</p>
            <button onClick={triggerSync} className="px-6 py-2 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700">Sync Now</button>
          </Card>
        )}

        <div className="grid grid-cols-5 gap-4 mb-6">
          <Card className="p-4"><Stat label="Tickets" value={portfolio?.total_tickets?.toLocaleString() || 0} /></Card>
          <Card className="p-4"><Stat label="Revenue" value={`$${portfolio?.total_revenue?.toLocaleString() || 0}`} /></Card>
          <Card className="p-4"><Stat label="Spend" value={`$${portfolio?.total_spend?.toLocaleString() || 0}`} /></Card>
          <Card className="p-4"><Stat label="CAC" value={`$${portfolio?.portfolio_cac?.toFixed(2) || '0.00'}`} /></Card>
          <Card className="p-4"><Stat label="Customers" value={cs?.total?.toLocaleString() || 0} /></Card>
        </div>

        {tab === 'events' && (
          <div className="grid grid-cols-12 gap-6">
            <div className="col-span-4 space-y-4 max-h-[70vh] overflow-auto">
              {events?.length ? events.map(e => <EventCard key={e.event_id} event={e} selected={selectedEvent?.event_id === e.event_id} onSelect={setSelectedEvent} />) : (
                <Card className="p-6 text-center text-gray-500">{syncing ? 'Loading events...' : 'No upcoming events found'}</Card>
              )}
            </div>
            <div className="col-span-8"><EventDetail event={selectedEvent} /></div>
          </div>
        )}

        {tab === 'crm' && (
          <div>
            {/* Sub-nav */}
            <div className="flex items-center gap-4 mb-6">
              <div className="flex bg-gray-100 rounded-lg p-1">
                <button onClick={() => setCrmView('targeting')} className={`px-4 py-2 rounded-lg text-sm font-medium ${crmView === 'targeting' ? 'bg-white shadow' : 'text-gray-600'}`}>
                  {'\u{1F3AF}'} Sell Tickets
                </button>
                <button onClick={() => setCrmView('browse')} className={`px-4 py-2 rounded-lg text-sm font-medium ${crmView === 'browse' ? 'bg-white shadow' : 'text-gray-600'}`}>
                  Browse Customers
                </button>
              </div>
              {crmView === 'targeting' && targeting && totalTargetable > 0 && (
                <button onClick={handleExportAll} className="ml-auto px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors">
                  Export All {totalTargetable.toLocaleString()} Contacts
                </button>
              )}
            </div>

            {/* ===== SELL TICKETS VIEW ===== */}
            {crmView === 'targeting' && (
              <div>
                {/* Event Selector */}
                <Card className="p-4 mb-6">
                  <div className="flex items-center gap-6">
                    <div className="flex-1">
                      <label className="text-sm font-semibold text-gray-700 mb-1 block">Target Event</label>
                      <select value={targetEvent} onChange={e => setTargetEvent(e.target.value)}
                        className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl text-sm font-medium bg-white focus:outline-none focus:border-blue-500 transition-colors">
                        <option value="">Select an upcoming event...</option>
                        {events?.map(e => (
                          <option key={e.event_id} value={e.event_id}>
                            {e.event_name} \u2014 {e.event_date?.slice(0, 10)} ({e.days_until}d) \u2014 {e.tickets_sold}/{e.capacity} sold
                          </option>
                        ))}
                      </select>
                    </div>
                    {targeting && (
                      <div className="flex gap-6 shrink-0">
                        <div className="text-center">
                          <div className="text-sm text-gray-500">Sold</div>
                          <div className="text-2xl font-bold text-green-600">{targeting.current_tickets?.toLocaleString()}</div>
                        </div>
                        <div className="text-center">
                          <div className="text-sm text-gray-500">Capacity</div>
                          <div className="text-2xl font-bold text-gray-400">{targeting.capacity?.toLocaleString()}</div>
                        </div>
                        <div className="text-center">
                          <div className="text-sm text-gray-500">{targeting.days_until}d left</div>
                          <div className="text-2xl font-bold text-blue-600">${targeting.avg_ticket_price?.toFixed(0)} avg</div>
                        </div>
                      </div>
                    )}
                  </div>
                </Card>

                {targetingLoading && <div className="text-center py-12 text-gray-500"><div className="text-lg mb-2">Analyzing audiences...</div></div>}

                {!targetingLoading && targeting && (
                  <>
                    {/* Intelligence Row: Revenue Gap + Repeat Rate + Quick Win */}
                    <div className="grid grid-cols-3 gap-4 mb-6">
                      {/* Revenue Gap */}
                      <Card className="p-4">
                        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Revenue Gap vs {rg.last_year || 'Last Year'}</div>
                        {rg.tickets_gap > 0 ? (
                          <>
                            <div className="text-3xl font-bold text-red-600 mb-1">{rg.tickets_gap?.toLocaleString()} tickets</div>
                            <div className="text-sm text-gray-600 mb-3">${rg.revenue_gap?.toLocaleString()} revenue to match last year</div>
                            <div className="h-3 bg-gray-100 rounded-full overflow-hidden">
                              <div className="h-full rounded-full bg-gradient-to-r from-blue-500 to-blue-600 transition-all" style={{ width: `${Math.min(100, rg.pct_of_last_year || 0)}%` }} />
                            </div>
                            <div className="text-xs text-gray-500 mt-1">{rg.pct_of_last_year?.toFixed(0)}% of {rg.last_year} ticket sales</div>
                          </>
                        ) : rg.last_year ? (
                          <div className="text-xl font-bold text-green-600">Ahead of {rg.last_year}! {'\u{1F389}'}</div>
                        ) : (
                          <div className="text-sm text-gray-400">No prior year data</div>
                        )}
                      </Card>

                      {/* Repeat Buyer Rate */}
                      <Card className="p-4">
                        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Repeat Buyer Rate</div>
                        <div className="text-3xl font-bold mb-1" style={{ color: rb.rate > 15 ? '#16a34a' : rb.rate > 8 ? '#d97706' : '#dc2626' }}>
                          {rb.rate?.toFixed(1) || 0}%
                        </div>
                        <div className="text-sm text-gray-600 mb-2">
                          {rb.rebought_from_last_year || 0} of {rb.last_year_buyers || 0} last year&apos;s buyers returned
                        </div>
                        <div className="h-3 bg-gray-100 rounded-full overflow-hidden mb-1">
                          <div className="h-full rounded-full bg-gradient-to-r from-green-400 to-green-600" style={{ width: `${Math.min(100, rb.last_year_rebuy_rate || 0)}%` }} />
                        </div>
                        <div className="text-xs text-gray-500">
                          {rb.total_past_buyers || 0} all-time past buyers &bull; {rb.count || 0} have rebought
                        </div>
                      </Card>

                      {/* Quick Win */}
                      <Card className="p-4 bg-gradient-to-br from-green-50 to-white border-green-200">
                        <div className="text-xs font-semibold text-green-700 uppercase tracking-wide mb-2">{'\u26A1'} Quick Win</div>
                        <div className="text-3xl font-bold text-green-700 mb-1">${qw.expected_revenue?.toLocaleString() || 0}</div>
                        <div className="text-sm text-gray-600 mb-2">
                          Email {qw.emails_to_send?.toLocaleString() || 0} {qw.audience || 'top'} past attendees
                        </div>
                        <div className="text-xs text-gray-500">
                          ~{qw.expected_tickets || 0} tickets at {qw.conversion_rate_used || 0}% conversion
                        </div>
                      </Card>
                    </div>

                    {/* Timing Recommendations */}
                    {targeting.timing_recommendations?.length > 0 && (
                      <Card className="p-4 mb-6">
                        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">{'\u23F0'} Timing Recommendations</div>
                        <div className="space-y-2">
                          {targeting.timing_recommendations.map((rec, i) => {
                            const u = URGENCY_COLORS[rec.urgency] || URGENCY_COLORS.soon;
                            const timingParam = rec.timing_segments?.join(',') || 'all';
                            const keyParam = targeting?.export_token ? `&key=${encodeURIComponent(targeting.export_token)}` : '';
                            return (
                              <div key={i} className="flex items-center gap-3 p-3 rounded-lg" style={{ backgroundColor: u.bg }}>
                                <span className="text-xs font-bold px-2 py-1 rounded shrink-0" style={{ backgroundColor: u.color, color: 'white' }}>{u.label}</span>
                                <span className="text-sm font-medium text-gray-800 flex-1">{rec.action}</span>
                                <button
                                  onClick={() => window.open(`${API_BASE}/api/export/csv?event_id=${targetEvent}&audience=timing&timing=${timingParam}${keyParam}`, '_blank')}
                                  className="shrink-0 flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-bold text-white transition-colors hover:opacity-90"
                                  style={{ backgroundColor: u.color }}>
                                  {'\u2B07\uFE0F'} Download {rec.count} emails
                                </button>
                              </div>
                            );
                          })}
                        </div>
                      </Card>
                    )}

                    {/* ===== INTELLIGENCE ENGINE ===== */}
                    {intel && !intelLoading && (
                      <Card className="p-4 mb-6">
                        <div className="flex items-center justify-between mb-4">
                          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wide">{'\u{1F9E0}'} Intelligence Engine</div>
                        </div>

                        {/* Velocity Alert — always visible */}
                        {intel.velocity && (
                          <div className={`p-4 rounded-lg mb-4 ${intel.velocity.will_sell_out ? 'bg-green-50 border border-green-200' : 'bg-amber-50 border border-amber-200'}`}>
                            <div className="flex items-center justify-between mb-2">
                              <span className="font-bold text-sm">{intel.velocity.will_sell_out ? '\u2705 On Pace to Sell Out' : '\u26A0\uFE0F Velocity Alert'}</span>
                              <span className="text-sm font-mono font-bold">{intel.velocity.current_velocity} tickets/day</span>
                            </div>
                            {!intel.velocity.will_sell_out && intel.velocity.tickets_remaining > 0 && (
                              <div className="text-sm text-gray-700 mb-2">
                                {intel.velocity.projected_unsold > 0
                                  ? `At current pace: ~${intel.velocity.projected_unsold} tickets unsold. Need ${Math.ceil(intel.velocity.tickets_remaining / Math.max(1, intel.days_until))} tickets/day to sell out.`
                                  : `${intel.velocity.tickets_remaining} remaining. ${intel.velocity.days_at_current_pace ? `${intel.velocity.days_at_current_pace} days at current pace.` : ''}`}
                              </div>
                            )}
                            {intel.velocity.gap_closing_plan?.length > 0 && (
                              <div className="mt-3 space-y-2">
                                <div className="text-xs font-semibold text-gray-500 uppercase">Gap-Closing Plan ({intel.velocity.total_recoverable} tickets recoverable)</div>
                                {intel.velocity.gap_closing_plan.map((p, i) => (
                                  <div key={i} className="flex items-center justify-between text-sm bg-white p-2 rounded">
                                    <span className="flex items-center gap-2">
                                      <span className="w-5 h-5 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs font-bold">{p.priority}</span>
                                      {p.action}
                                    </span>
                                    <span className="font-bold text-green-600">+{p.expected_tickets} tickets</span>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        )}

                        {/* Revenue Projection */}
                        {intel.revenue_projection && (
                          <div className="p-4 rounded-lg mb-4 bg-blue-50 border border-blue-200">
                            <div className="flex items-center justify-between">
                              <div>
                                <div className="text-xs font-semibold text-blue-700 uppercase mb-1">Revenue Projection</div>
                                <div className="text-2xl font-bold text-blue-700">${intel.revenue_projection.projected_revenue?.toLocaleString()}</div>
                                <div className="text-xs text-gray-600">Range: ${intel.revenue_projection.projected_revenue_range?.[0]?.toLocaleString()} — ${intel.revenue_projection.projected_revenue_range?.[1]?.toLocaleString()}</div>
                              </div>
                              <div className="text-right">
                                <div className="text-sm text-gray-600">{intel.revenue_projection.sell_through_pct?.toFixed(1)}% sold</div>
                                <div className="text-sm text-gray-600">{intel.revenue_projection.projected_final_tickets?.toLocaleString()} projected tickets</div>
                                <div className="text-xs text-gray-400">{Math.round(intel.revenue_projection.confidence * 100)}% confidence</div>
                              </div>
                            </div>
                          </div>
                        )}

                        {/* Collapsible Intelligence Sections */}
                        <div className="space-y-2">

                          {/* Cross-Sell */}
                          {intel.cross_sell?.candidates > 0 && (
                            <div className="border rounded-lg overflow-hidden">
                              <button onClick={() => toggleIntel('cross_sell')} className="w-full flex items-center justify-between p-3 bg-gray-50 hover:bg-gray-100 transition-colors text-left">
                                <span className="font-semibold text-sm">{'\u{1F504}'} Cross-Sell — {intel.cross_sell.candidates} candidates</span>
                                <span className="text-xs text-gray-500">{intelOpen.cross_sell ? '\u25B2' : '\u25BC'}</span>
                              </button>
                              {intelOpen.cross_sell && (
                                <div className="p-3 text-sm">
                                  <p className="text-gray-600 mb-2">{intel.cross_sell.recommendation}</p>
                                  <div className="flex flex-wrap gap-2 mb-3">
                                    {Object.entries(intel.cross_sell.by_source_type || {}).map(([type, count]) => (
                                      <span key={type} className="px-2 py-1 bg-purple-100 text-purple-700 rounded-full text-xs font-medium">{type}: {count}</span>
                                    ))}
                                  </div>
                                  <button onClick={() => handleIntelExport('cross_sell')} className="px-3 py-1.5 bg-purple-600 text-white rounded-lg text-xs font-bold hover:bg-purple-700">{'\u2B07\uFE0F'} Download {intel.cross_sell.candidates} emails</button>
                                </div>
                              )}
                            </div>
                          )}

                          {/* Super-Spreaders */}
                          {intel.super_spreaders?.total_spreaders > 0 && (
                            <div className="border rounded-lg overflow-hidden">
                              <button onClick={() => toggleIntel('spreaders')} className="w-full flex items-center justify-between p-3 bg-gray-50 hover:bg-gray-100 transition-colors text-left">
                                <span className="font-semibold text-sm">{'\u{1F4E3}'} Super-Spreaders — {intel.super_spreaders.total_spreaders} identified ({intel.super_spreaders.total_estimated_plus_ones} est. +1s)</span>
                                <span className="text-xs text-gray-500">{intelOpen.spreaders ? '\u25B2' : '\u25BC'}</span>
                              </button>
                              {intelOpen.spreaders && (
                                <div className="p-3 text-sm">
                                  <p className="text-gray-600 mb-2">{intel.super_spreaders.recommendation}</p>
                                  <button onClick={() => handleIntelExport('super_spreaders')} className="px-3 py-1.5 bg-orange-600 text-white rounded-lg text-xs font-bold hover:bg-orange-700">{'\u2B07\uFE0F'} Download {intel.super_spreaders.total_spreaders} spreader emails</button>
                                </div>
                              )}
                            </div>
                          )}

                          {/* VIPs */}
                          {intel.vips?.vips_not_bought > 0 && (
                            <div className="border rounded-lg overflow-hidden">
                              <button onClick={() => toggleIntel('vips')} className="w-full flex items-center justify-between p-3 bg-gray-50 hover:bg-gray-100 transition-colors text-left">
                                <span className="font-semibold text-sm">{'\u{1F451}'} VIPs Not Purchased — {intel.vips.vips_not_bought} (${intel.vips.vip_total_value?.toLocaleString()} lifetime value)</span>
                                <span className="text-xs text-gray-500">{intelOpen.vips ? '\u25B2' : '\u25BC'}</span>
                              </button>
                              {intelOpen.vips && (
                                <div className="p-3 text-sm">
                                  <p className="text-gray-600 mb-2">{intel.vips.recommendation}</p>
                                  <button onClick={() => handleIntelExport('vips')} className="px-3 py-1.5 bg-yellow-600 text-white rounded-lg text-xs font-bold hover:bg-yellow-700">{'\u2B07\uFE0F'} Download {intel.vips.vips_not_bought} VIP emails</button>
                                </div>
                              )}
                            </div>
                          )}

                          {/* Churn Prediction */}
                          {intel.churn_prediction?.total_at_risk > 0 && (
                            <div className="border rounded-lg overflow-hidden">
                              <button onClick={() => toggleIntel('churn')} className="w-full flex items-center justify-between p-3 bg-gray-50 hover:bg-gray-100 transition-colors text-left">
                                <span className="font-semibold text-sm">{'\u23F3'} Churn Risk — {intel.churn_prediction.critical} critical, {intel.churn_prediction.urgent} urgent (${intel.churn_prediction.total_value_at_risk?.toLocaleString()} at risk)</span>
                                <span className="text-xs text-gray-500">{intelOpen.churn ? '\u25B2' : '\u25BC'}</span>
                              </button>
                              {intelOpen.churn && (
                                <div className="p-3 text-sm">
                                  <p className="text-gray-600 mb-2">{intel.churn_prediction.recommendation}</p>
                                  <div className="flex gap-3 mb-3">
                                    <span className="px-2 py-1 bg-red-100 text-red-700 rounded-full text-xs font-bold">Critical: {intel.churn_prediction.critical}</span>
                                    <span className="px-2 py-1 bg-orange-100 text-orange-700 rounded-full text-xs font-bold">Urgent: {intel.churn_prediction.urgent}</span>
                                    <span className="px-2 py-1 bg-yellow-100 text-yellow-700 rounded-full text-xs font-bold">Watch: {intel.churn_prediction.watch}</span>
                                  </div>
                                  <button onClick={() => handleIntelExport('churn_critical')} className="px-3 py-1.5 bg-red-600 text-white rounded-lg text-xs font-bold hover:bg-red-700">{'\u2B07\uFE0F'} Download {intel.churn_prediction.critical + intel.churn_prediction.urgent} urgent save emails</button>
                                </div>
                              )}
                            </div>
                          )}

                          {/* Purchase Velocity Triggers */}
                          {intel.purchase_timing?.optimal_followup_days && (
                            <div className="border rounded-lg overflow-hidden">
                              <button onClick={() => toggleIntel('velocity')} className="w-full flex items-center justify-between p-3 bg-gray-50 hover:bg-gray-100 transition-colors text-left">
                                <span className="font-semibold text-sm">{'\u{1F3C3}'} Purchase Velocity — optimal follow-up: {intel.purchase_timing.optimal_followup_days} days post-event</span>
                                <span className="text-xs text-gray-500">{intelOpen.velocity ? '\u25B2' : '\u25BC'}</span>
                              </button>
                              {intelOpen.velocity && (
                                <div className="p-3 text-sm">
                                  <p className="text-gray-600 mb-2">{intel.purchase_timing.recommendation}</p>
                                  <div className="grid grid-cols-6 gap-2">
                                    {Object.entries(intel.purchase_timing.buckets || {}).map(([bucket, count]) => (
                                      <div key={bucket} className="text-center p-2 bg-gray-50 rounded">
                                        <div className="text-xs text-gray-500">{bucket}d</div>
                                        <div className="font-bold text-sm">{count}</div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>
                          )}

                          {/* Promo Intelligence */}
                          {intel.promo_intelligence?.by_segment && Object.keys(intel.promo_intelligence.by_segment).length > 0 && (
                            <div className="border rounded-lg overflow-hidden">
                              <button onClick={() => toggleIntel('promo')} className="w-full flex items-center justify-between p-3 bg-gray-50 hover:bg-gray-100 transition-colors text-left">
                                <span className="font-semibold text-sm">{'\u{1F3F7}\uFE0F'} Promo Intelligence — who needs discounts vs. who doesn&apos;t</span>
                                <span className="text-xs text-gray-500">{intelOpen.promo ? '\u25B2' : '\u25BC'}</span>
                              </button>
                              {intelOpen.promo && (
                                <div className="p-3 text-sm">
                                  <div className="grid grid-cols-3 gap-2">
                                    {Object.entries(intel.promo_intelligence.by_segment).map(([seg, data]) => (
                                      <div key={seg} className="p-2 bg-gray-50 rounded">
                                        <div className="font-semibold text-xs capitalize mb-1">{seg.replace(/_/g, ' ')}</div>
                                        <div className="text-xs text-gray-600">Promo rate: {data.promo_rate}%</div>
                                        <div className="text-xs text-gray-600">Full: ${data.avg_full_price} | Promo: ${data.avg_promo_price}</div>
                                        <div className={`text-xs mt-1 font-medium ${data.promo_rate < 15 ? 'text-green-600' : 'text-orange-600'}`}>{data.recommendation}</div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>
                          )}

                          {/* Cannibalization */}
                          {intel.cannibalization?.count > 0 && (
                            <div className="border border-red-200 rounded-lg overflow-hidden">
                              <button onClick={() => toggleIntel('cannibal')} className="w-full flex items-center justify-between p-3 bg-red-50 hover:bg-red-100 transition-colors text-left">
                                <span className="font-semibold text-sm text-red-700">{'\u26A0\uFE0F'} Cannibalization Risk — {intel.cannibalization.count} events too close</span>
                                <span className="text-xs text-gray-500">{intelOpen.cannibal ? '\u25B2' : '\u25BC'}</span>
                              </button>
                              {intelOpen.cannibal && (
                                <div className="p-3 text-sm">
                                  <p className="text-gray-600 mb-2">{intel.cannibalization.recommendation}</p>
                                  {intel.cannibalization.risk_events?.map((r, i) => (
                                    <div key={i} className="p-2 bg-white rounded mb-1 flex justify-between items-center">
                                      <span>{r.event_name} ({r.days_apart > 0 ? '+' : ''}{r.days_apart}d)</span>
                                      <span className="text-xs text-red-600 font-bold">{r.buyer_overlap} shared buyers ({r.overlap_pct}%)</span>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}

                          {/* Competitor Radar */}
                          {intel.competitors?.count > 0 && (
                            <div className="border border-amber-200 rounded-lg overflow-hidden">
                              <button onClick={() => toggleIntel('competitors')} className="w-full flex items-center justify-between p-3 bg-amber-50 hover:bg-amber-100 transition-colors text-left">
                                <span className="font-semibold text-sm text-amber-700">{'\u{1F4E1}'} Competitor Radar — {intel.competitors.count} nearby events</span>
                                <span className="text-xs text-gray-500">{intelOpen.competitors ? '\u25B2' : '\u25BC'}</span>
                              </button>
                              {intelOpen.competitors && (
                                <div className="p-3 text-sm">
                                  <p className="text-gray-600 mb-2">{intel.competitors.recommendation}</p>
                                  {intel.competitors.competing_events?.map((c, i) => (
                                    <div key={i} className="p-2 bg-white rounded mb-1 flex justify-between items-center">
                                      <span>{c.event_name} <span className="text-xs text-gray-400">({c.event_type})</span></span>
                                      <span className={`text-xs font-bold ${c.same_weekend ? 'text-red-600' : 'text-amber-600'}`}>{c.same_weekend ? 'SAME WEEKEND' : `${c.days_apart > 0 ? '+' : ''}${c.days_apart}d`}</span>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      </Card>
                    )}

                    {/* Audience Cards */}
                    <div className="grid grid-cols-4 gap-4 mb-6">
                      {Object.entries(targeting.audiences || {}).map(([key, data]) => (
                        <AudienceCard key={key} audienceKey={key} data={data} selected={selectedAudience}
                          onSelect={setSelectedAudience} onExport={handleExportCSV} />
                      ))}
                    </div>

                    {/* Audience Detail Table */}
                    {selectedAudience && audienceData && (
                      <Card className="overflow-hidden">
                        <div className="p-4 border-b flex items-center justify-between" style={{ backgroundColor: AUDIENCE_COLORS[selectedAudience]?.bg }}>
                          <div>
                            <h3 className="font-bold text-gray-900">{AUDIENCE_ICONS[selectedAudience]} {audienceData.label}</h3>
                            <p className="text-sm text-gray-500">Showing top {Math.min(audienceCustomers.length, 100)} of {audienceData.total_available?.toLocaleString()} &mdash; sorted by conversion likelihood</p>
                          </div>
                          <button onClick={() => handleExportCSV(selectedAudience)}
                            className="px-4 py-2 rounded-lg text-sm font-medium text-white" style={{ backgroundColor: AUDIENCE_COLORS[selectedAudience]?.color }}>
                            Export {audienceData.total_available?.toLocaleString()} to CSV
                          </button>
                        </div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-sm">
                            <thead className="bg-gray-50 text-left font-semibold text-gray-600">
                              <tr>
                                <th className="px-4 py-3">Email</th>
                                <th className="px-4 py-3">Segment</th>
                                <th className="px-4 py-3">Timing</th>
                                <th className="px-4 py-3">City</th>
                                <th className="px-4 py-3 text-right">Events</th>
                                <th className="px-4 py-3 text-right">Spent</th>
                                <th className="px-4 py-3 text-right">LTV</th>
                                <th className="px-4 py-3 text-right">Inactive</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y">
                              {audienceCustomers.map((c, i) => {
                                const seg = SEGMENTS[c.rfm_segment] || SEGMENTS.other;
                                return (
                                  <tr key={i} className="hover:bg-gray-50 cursor-pointer" onClick={() => handleSelectCustomer(c.email)}>
                                    <td className="px-4 py-3 font-medium truncate max-w-[200px]" title={c.email}>{c.email}</td>
                                    <td className="px-4 py-3"><Badge color={seg.color} bg={seg.color + '20'}>{seg.label}</Badge></td>
                                    <td className="px-4 py-3 text-xs text-gray-500 capitalize">{(c.timing_segment || '').replace(/_/g, ' ')}</td>
                                    <td className="px-4 py-3 text-gray-600">{c.favorite_city || '\u2014'}</td>
                                    <td className="px-4 py-3 text-right">{c.total_events}</td>
                                    <td className="px-4 py-3 text-right">${c.total_spent?.toFixed(0)}</td>
                                    <td className="px-4 py-3 text-right font-bold text-blue-600">{c.ltv_score?.toFixed(0)}</td>
                                    <td className="px-4 py-3 text-right text-gray-500">{c.days_since_last}d</td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                      </Card>
                    )}

                    {!selectedAudience && (
                      <Card className="p-6 text-center text-gray-400">
                        <div className="mb-1">Click an audience card to see customers, or export directly to CSV</div>
                      </Card>
                    )}
                  </>
                )}

                {!targetEvent && (
                  <Card className="p-12 text-center">
                    <div className="text-4xl mb-4">{'\u{1F3AF}'}</div>
                    <div className="text-xl font-bold text-gray-700 mb-2">Event Targeting</div>
                    <p className="text-gray-500 max-w-md mx-auto">Select an event to see revenue gaps, repeat buyer rates, and targetable audiences with one-click CSV export.</p>
                  </Card>
                )}
              </div>
            )}

            {/* ===== BROWSE VIEW ===== */}
            {crmView === 'browse' && (
              <div>
                <div className="grid grid-cols-6 gap-4 mb-6">
                  {Object.entries(cs?.segments || {}).map(([s, n]) => {
                    const cfg = SEGMENTS[s] || SEGMENTS.other;
                    const isActive = crmSegment === s;
                    return (
                      <Card key={s} className={`p-4 text-center cursor-pointer hover:shadow-md ${isActive ? 'ring-2 ring-blue-500' : ''}`}
                            onClick={() => { setCrmSegment(isActive ? '' : s); setCrmPage(0); }}>
                        <div className="text-2xl font-bold" style={{ color: cfg.color }}>{n}</div>
                        <div className="text-sm text-gray-500">{cfg.label}</div>
                      </Card>
                    );
                  })}
                </div>
                <Card className="overflow-hidden">
                  <div className="p-4 border-b bg-gray-50 flex flex-wrap gap-3 items-center">
                    <input type="text" placeholder="Search email..." value={crmSearch} onChange={e => { setCrmSearch(e.target.value); setCrmPage(0); }}
                      className="px-3 py-2 border rounded-lg text-sm w-64 focus:outline-none focus:ring-2 focus:ring-blue-500" />
                    <select value={crmCity} onChange={e => { setCrmCity(e.target.value); setCrmPage(0); }}
                      className="px-3 py-2 border rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500">
                      <option value="">All Cities</option>
                      {crmCities.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                    <select value={crmType} onChange={e => { setCrmType(e.target.value); setCrmPage(0); }}
                      className="px-3 py-2 border rounded-lg text-sm bg-white capitalize focus:outline-none focus:ring-2 focus:ring-blue-500">
                      <option value="">All Types</option>
                      {crmTypes.map(t => <option key={t} value={t} className="capitalize">{t}</option>)}
                    </select>
                    {(crmSearch || crmSegment || crmCity || crmType) && (
                      <button onClick={() => { setCrmSearch(''); setCrmSegment(''); setCrmCity(''); setCrmType(''); setCrmPage(0); }}
                        className="px-3 py-2 text-sm text-red-600 hover:bg-red-50 rounded-lg">Clear filters</button>
                    )}
                    <div className="ml-auto text-sm text-gray-500">{customerTotal} customer{customerTotal !== 1 ? 's' : ''}</div>
                  </div>
                  <div className="overflow-x-auto">
                    <CustomerTable customers={customers} onSelect={handleSelectCustomer} sortBy={crmSort} sortOrder={crmOrder}
                      onSort={(field) => { if (crmSort === field) setCrmOrder(crmOrder === 'DESC' ? 'ASC' : 'DESC'); else { setCrmSort(field); setCrmOrder('DESC'); } setCrmPage(0); }} />
                  </div>
                  {customerTotal > CRM_LIMIT && (
                    <div className="p-4 border-t flex items-center justify-between">
                      <button onClick={() => setCrmPage(Math.max(0, crmPage - 1))} disabled={crmPage === 0}
                        className={`px-4 py-2 rounded-lg text-sm font-medium ${crmPage === 0 ? 'text-gray-300' : 'text-blue-600 hover:bg-blue-50'}`}>Previous</button>
                      <span className="text-sm text-gray-500">Page {crmPage + 1} of {Math.ceil(customerTotal / CRM_LIMIT)}</span>
                      <button onClick={() => setCrmPage(crmPage + 1)} disabled={(crmPage + 1) * CRM_LIMIT >= customerTotal}
                        className={`px-4 py-2 rounded-lg text-sm font-medium ${(crmPage + 1) * CRM_LIMIT >= customerTotal ? 'text-gray-300' : 'text-blue-600 hover:bg-blue-50'}`}>Next</button>
                    </div>
                  )}
                </Card>
              </div>
            )}
          </div>
        )}

        {tab === 'overlap' && (
          <div>
            {overlapLoading && <div className="text-center py-12 text-gray-500">Analyzing attendee overlap across all events...</div>}
            {overlap && !overlapLoading && (
              <div className="space-y-6">
                {/* Summary Banner */}
                <div className="grid grid-cols-4 gap-4">
                  <Card className="p-4"><Stat label="Events Analyzed" value={overlap.summary?.total_editions || 0} /></Card>
                  <Card className="p-4"><Stat label="Overlap Pairs" value={overlap.summary?.total_pairs || 0} /></Card>
                  <Card className="p-4"><Stat label="Cross-Type Pairs" value={overlap.summary?.cross_type_pairs || 0} /></Card>
                  <Card className="p-4"><Stat label="Cities" value={overlap.summary?.cities_analyzed || 0} /></Card>
                </div>

                {/* Top Insight */}
                {overlap.summary?.highest_cross_type && (
                  <Card className="p-4 border-l-4 border-purple-500">
                    <div className="text-sm font-medium text-purple-700 mb-1">Biggest Cross-Event Overlap</div>
                    <div className="text-lg font-bold">{overlap.summary.highest_cross_type.event_a} & {overlap.summary.highest_cross_type.event_b}</div>
                    <div className="text-sm text-gray-600">{overlap.summary.highest_cross_type.overlap_count.toLocaleString()} shared attendees in {overlap.summary.highest_cross_type.city}</div>
                  </Card>
                )}

                {/* City selector + view toggle */}
                <div className="flex items-center justify-between">
                  <div className="flex gap-2 flex-wrap">
                    {overlap.cities?.map(c => (
                      <button key={c} onClick={() => setOverlapCity(c)} className={`px-4 py-2 rounded-lg text-sm font-medium ${overlapCity === c ? 'bg-purple-600 text-white' : 'bg-white text-gray-600 border hover:bg-gray-50'}`}>{c}</button>
                    ))}
                  </div>
                  <div className="flex bg-gray-100 rounded-lg p-1">
                    <button onClick={() => setOverlapView('matrix')} className={`px-3 py-1 rounded text-sm font-medium ${overlapView === 'matrix' ? 'bg-white shadow' : 'text-gray-600'}`}>Matrix</button>
                    <button onClick={() => setOverlapView('pairs')} className={`px-3 py-1 rounded text-sm font-medium ${overlapView === 'pairs' ? 'bg-white shadow' : 'text-gray-600'}`}>Ranked</button>
                  </div>
                </div>

                {/* Matrix View */}
                {overlapView === 'matrix' && overlap.matrices?.[overlapCity] && (() => {
                  const m = overlap.matrices[overlapCity];
                  const maxOverlap = Math.max(...m.matrix.flat().filter((_, idx) => Math.floor(idx / m.labels.length) !== idx % m.labels.length), 1);
                  return (
                    <Card className="p-0 overflow-auto">
                      <div className="p-4 border-b"><h3 className="font-bold text-lg">{overlapCity} — Attendee Overlap Matrix</h3><p className="text-sm text-gray-500">Each cell shows shared attendees between two events. Diagonal shows total unique attendees.</p></div>
                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr>
                              <th className="p-2 text-left bg-gray-50 sticky left-0 z-10 min-w-[180px]"></th>
                              {m.labels.map((label, i) => (
                                <th key={i} className="p-2 text-center bg-gray-50 min-w-[100px]" style={{writingMode: 'vertical-lr', transform: 'rotate(180deg)', height: '160px'}}>
                                  <span className="text-xs font-medium">{label.length > 25 ? label.slice(0, 25) + '...' : label}</span>
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {m.labels.map((rowLabel, i) => (
                              <tr key={i} className="border-t">
                                <td className="p-2 font-medium bg-gray-50 sticky left-0 z-10 text-xs">{rowLabel}<span className="block text-gray-400">{m.counts[i].toLocaleString()} attendees</span></td>
                                {m.matrix[i].map((val, j) => {
                                  const isDiag = i === j;
                                  const intensity = isDiag ? 0 : Math.min(val / maxOverlap, 1);
                                  const pctOfRow = isDiag ? 100 : m.counts[i] > 0 ? Math.round(val / m.counts[i] * 100) : 0;
                                  const bg = isDiag ? '#f3f4f6' : val === 0 ? '#fff' : `rgba(124, 58, 237, ${0.08 + intensity * 0.55})`;
                                  const textColor = isDiag ? '#374151' : intensity > 0.5 ? '#fff' : '#374151';
                                  return (
                                    <td key={j} className="p-2 text-center border-l" style={{backgroundColor: bg, color: textColor}} title={`${rowLabel} ∩ ${m.labels[j]}: ${val} attendees (${pctOfRow}% of ${rowLabel})`}>
                                      <div className="font-bold text-sm">{val > 0 ? val.toLocaleString() : '-'}</div>
                                      {!isDiag && val > 0 && <div className="text-xs opacity-75">{pctOfRow}%</div>}
                                    </td>
                                  );
                                })}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </Card>
                  );
                })()}

                {overlapView === 'matrix' && !overlap.matrices?.[overlapCity] && (
                  <Card className="p-8 text-center text-gray-500">Not enough events in {overlapCity} to build a matrix (need at least 2 unique events).</Card>
                )}

                {/* Ranked Pairs View */}
                {overlapView === 'pairs' && (
                  <Card className="p-0">
                    <div className="p-4 border-b"><h3 className="font-bold text-lg">{overlapCity} — Ranked Overlap Pairs</h3><p className="text-sm text-gray-500">All event pairs sorted by shared attendees. Cross-type pairs highlighted.</p></div>
                    <div className="max-h-[60vh] overflow-auto">
                      {(overlap.pairs_by_city?.[overlapCity] || []).length === 0 && (
                        <div className="p-8 text-center text-gray-500">No overlap pairs found for {overlapCity}.</div>
                      )}
                      {(overlap.pairs_by_city?.[overlapCity] || []).map((pair, idx) => (
                        <div key={idx} className={`p-4 border-b flex items-center gap-4 ${!pair.same_event ? 'bg-purple-50' : ''}`}>
                          <div className="text-2xl font-bold text-gray-300 w-8">#{idx + 1}</div>
                          <div className="flex-1">
                            <div className="flex items-center gap-2 mb-1">
                              <span className="font-semibold text-sm">{pair.event_a} ({pair.event_a_year})</span>
                              <span className="text-gray-400">&</span>
                              <span className="font-semibold text-sm">{pair.event_b} ({pair.event_b_year})</span>
                              {!pair.same_event && <span className="text-xs px-2 py-0.5 bg-purple-100 text-purple-700 rounded-full font-medium">Cross-Type</span>}
                              {pair.same_event && <span className="text-xs px-2 py-0.5 bg-blue-100 text-blue-700 rounded-full font-medium">Retention</span>}
                            </div>
                            <div className="flex gap-4 text-xs text-gray-500">
                              <span>{pair.overlap_count.toLocaleString()} shared attendees</span>
                              <span>{pair.pct_of_a}% of {pair.event_a}</span>
                              <span>{pair.pct_of_b}% of {pair.event_b}</span>
                            </div>
                          </div>
                          <div className="text-right">
                            <div className="text-xl font-bold text-purple-700">{pair.overlap_count.toLocaleString()}</div>
                            <div className="text-xs text-gray-400">shared</div>
                          </div>
                          <div className="w-32">
                            <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                              <div className="h-full rounded-full" style={{width: `${Math.max(pair.pct_of_a, pair.pct_of_b)}%`, backgroundColor: pair.same_event ? '#3b82f6' : '#7c3aed'}} />
                            </div>
                            <div className="text-xs text-gray-400 text-center mt-1">{Math.max(pair.pct_of_a, pair.pct_of_b)}%</div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </Card>
                )}

                {/* Global Top Cross-Type Pairs */}
                {overlap.top_cross_type?.length > 0 && (
                  <Card className="p-0">
                    <div className="p-4 border-b"><h3 className="font-bold text-lg">Top Cross-Type Overlaps (All Cities)</h3><p className="text-sm text-gray-500">Your best cross-sell audiences — attendees who go to different event types.</p></div>
                    <div className="max-h-[40vh] overflow-auto">
                      {overlap.top_cross_type.slice(0, 20).map((pair, idx) => (
                        <div key={idx} className="p-3 border-b flex items-center gap-3 hover:bg-purple-50">
                          <div className="text-lg font-bold text-gray-300 w-7">#{idx + 1}</div>
                          <div className="flex-1">
                            <div className="font-medium text-sm">{pair.event_a} & {pair.event_b}</div>
                            <div className="text-xs text-gray-500">{pair.city} — {pair.overlap_count.toLocaleString()} shared ({pair.pct_of_a}% / {pair.pct_of_b}%)</div>
                          </div>
                          <div className="font-bold text-purple-700">{pair.overlap_count.toLocaleString()}</div>
                        </div>
                      ))}
                    </div>
                  </Card>
                )}
              </div>
            )}
          </div>
        )}
      </main>

      {decisions?.pivot > 0 && (
        <div className="fixed bottom-0 left-0 right-0 bg-red-600 text-white py-3 px-6 z-50">
          <div className="max-w-7xl mx-auto flex items-center justify-between">
            <span className="font-bold">{'\u{1F6A8}'} {decisions.pivot} event(s) require PIVOT</span>
            <button onClick={() => setTab('events')} className="px-4 py-1 bg-white text-red-600 rounded font-bold">View</button>
          </div>
        </div>
      )}

      {selectedCustomer && <CustomerModal customer={selectedCustomer} orders={customerOrders} onClose={() => setSelectedCustomer(null)} />}
    </div>
  );
}
