import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

const API_BASE = 'https://craft-dominant-production.up.railway.app';

const DECISIONS = {
  pivot: { color: '#dc2626', bg: '#fef2f2', icon: 'ð¨', label: 'PIVOT' },
  push: { color: '#ea580c', bg: '#fff7ed', icon: 'ð', label: 'PUSH' },
  maintain: { color: '#16a34a', bg: '#f0fdf4', icon: 'â', label: 'MAINTAIN' },
  coast: { color: '#2563eb', bg: '#eff6ff', icon: 'ð', label: 'COAST' },
  not_started: { color: '#6b7280', bg: '#f3f4f6', icon: 'â³', label: 'FUTURE' },
};

const SEGMENTS = {
  champion: { color: '#16a34a', label: 'Champions' },
  loyal: { color: '#2563eb', label: 'Loyal' },
  potential: { color: '#8b5cf6', label: 'Potential' },
  at_risk: { color: '#ea580c', label: 'At Risk' },
  hibernating: { color: '#dc2626', label: 'Hibernating' },
  other: { color: '#6b7280', label: 'Other' },
};

const Card = ({ children, className = '', onClick }) => (
  <div className={`bg-white rounded-xl shadow-sm border border-gray-100 ${className} ${onClick ? 'cursor-pointer hover:shadow-md' : ''}`} onClick={onClick}>
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
          <p className="text-gray-500">{event.event_date?.slice(0, 10)} â¢ {event.days_until} days</p>
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
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-gray-500">
                <th className="text-left py-2">Year</th>
                <th className="text-right py-2">@ {event.days_until}d out</th>
                <th className="text-right py-2">Final</th>
                <th className="text-right py-2">Capacity</th>
              </tr>
            </thead>
            <tbody>
              {event.historical_comparisons?.map((h, i) => (
                <tr key={i} className="border-b border-gray-100">
                  <td className="py-2">
                    <div className="font-medium">{h.year}</div>
                    <div className="text-xs text-gray-400">{h.event_date?.slice(0, 10)}</div>
                  </td>
                  <td className="text-right py-2 font-medium">
                    {h.at_days_out ? `${h.at_days_out.tickets.toLocaleString()} (${h.at_days_out.sell_through?.toFixed(1)}%)` : '\u2014'}
                  </td>
                  <td className="text-right py-2">
                    {h.final_tickets?.toLocaleString()} ({h.final_sell_through}%)
                  </td>
                  <td className="text-right py-2 text-gray-500">
                    {h.capacity?.toLocaleString()}
                  </td>
                </tr>
              ))}
              <tr className="font-bold bg-blue-50">
                <td className="py-2 rounded-l">
                  <div>{new Date(event.event_date).getFullYear()} <span className="text-xs font-normal text-gray-500">current</span></div>
                  <div className="text-xs text-gray-400 font-normal">{event.event_date?.slice(0, 10)}</div>
                </td>
                <td className="text-right py-2 text-blue-600">
                  {event.tickets_sold?.toLocaleString()} ({event.sell_through?.toFixed(1)}%)
                </td>
                <td className="text-right py-2 text-blue-600">
                  {event.projected_final?.toLocaleString()} proj
                </td>
                <td className="text-right py-2 text-gray-500 rounded-r">
                  {event.capacity?.toLocaleString()}
                </td>
              </tr>
            </tbody>
          </table>
          {event.pace_vs_historical !== 0 && event.historical_median_at_point > 0 && (
            <div className="mt-3 text-sm">
              Pace vs median: <strong className={event.pace_vs_historical >= 0 ? 'text-green-600' : 'text-red-600'}>{event.pace_vs_historical >= 0 ? '+' : ''}{event.pace_vs_historical?.toFixed(0)}%</strong>
            </div>
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
              <span className="w-5 h-5 rounded-full flex items-center justify-center text-xs text-white font-bold" style={{ backgroundColor: cfg.color }}>{i + 1}</span>
              {a}
            </li>
          ))}
        </ul>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-4">
        <div className="p-3 bg-green-50 rounded-lg text-center">
          <div className="text-xl font-bold text-green-600">{event.high_value_targets}</div>
          <div className="text-xs">High-Value Targets</div>
        </div>
        <div className="p-3 bg-orange-50 rounded-lg text-center">
          <div className="text-xl font-bold text-orange-600">{event.reactivation_targets}</div>
          <div className="text-xs">Reactivation</div>
        </div>
      </div>
    </Card>
  );
};

const CustomerTable = ({ customers, onSelect }) => (
  <table className="w-full">
    <thead className="bg-gray-50 text-left text-sm font-semibold text-gray-600">
      <tr>
        <th className="px-4 py-3">Email</th>
        <th className="px-4 py-3">Segment</th>
        <th className="px-4 py-3 text-right">Orders</th>
        <th className="px-4 py-3 text-right">Spent</th>
        <th className="px-4 py-3 text-right">LTV</th>
      </tr>
    </thead>
    <tbody className="divide-y">
      {customers.map((c, i) => {
        const seg = SEGMENTS[c.rfm_segment] || SEGMENTS.other;
        return (
          <tr key={i} className="hover:bg-gray-50 cursor-pointer" onClick={() => onSelect(c.email)}>
            <td className="px-4 py-3 text-sm font-medium">{c.email}</td>
            <td className="px-4 py-3"><Badge color={seg.color} bg={seg.color + '20'}>{seg.label}</Badge></td>
            <td className="px-4 py-3 text-right">{c.total_orders}</td>
            <td className="px-4 py-3 text-right">${c.total_spent?.toFixed(0)}</td>
            <td className="px-4 py-3 text-right font-bold text-blue-600">{c.ltv_score?.toFixed(0)}</td>
          </tr>
        );
      })}
    </tbody>
  </table>
);

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
            <div key={i} className="text-center p-4">
              <div className="text-2xl font-bold">{s.v}</div>
              <div className="text-sm text-gray-500">{s.l}</div>
            </div>
          ))}
        </div>

        <div className="p-5">
          <div className="grid grid-cols-2 gap-4 mb-4">
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs text-gray-500">Favorite Type</div>
              <div className="font-semibold">{customer.favorite_event_type || 'N/A'}</div>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs text-gray-500">Favorite City</div>
              <div className="font-semibold">{customer.favorite_city || 'N/A'}</div>
            </div>
          </div>

          <h3 className="font-semibold mb-3">Order History ({orders?.length || 0})</h3>
          <div className="space-y-2 max-h-60 overflow-auto">
            {orders?.map((o, i) => (
              <div key={i} className="flex justify-between p-3 bg-gray-50 rounded-lg text-sm">
                <div>
                  <div className="font-medium">{o.event_name}</div>
                  <div className="text-gray-500">{o.order_timestamp?.slice(0, 10)}</div>
                </div>
                <div className="text-right">
                  <div className="font-medium">${o.gross_amount?.toFixed(2)}</div>
                  <div className="text-gray-500">{o.ticket_count} ticket(s)</div>
                </div>
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
  const [selectedEvent, setSelectedEvent] = useState(null);
  const [selectedCustomer, setSelectedCustomer] = useState(null);
  const [customerOrders, setCustomerOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [syncError, setSyncError] = useState(null);

  const fetchDashboard = () => {
    fetch(`${API_BASE}/api/dashboard`)
      .then(r => r.json())
      .then(d => {
        setDashboard(d);
        if (d.events?.length) setSelectedEvent(d.events[0]);
        setLoading(false);
        // Check if backend is still syncing
        if (d.sync?.running) {
          setSyncing(true);
          // Poll every 10 seconds until sync is done
          setTimeout(fetchDashboard, 10000);
        } else {
          setSyncing(false);
          if (d.sync?.error) setSyncError(d.sync.error);
        }
      })
      .catch(() => setLoading(false));
  };

  useEffect(() => {
    fetchDashboard();
  }, []);

  useEffect(() => {
    if (tab === 'crm') {
      fetch(`${API_BASE}/api/customers?limit=50`)
        .then(r => r.json())
        .then(d => setCustomers(d.customers || []));
    }
  }, [tab]);

  const handleSelectCustomer = (email) => {
    fetch(`${API_BASE}/api/customers/${encodeURIComponent(email)}`)
      .then(r => r.json())
      .then(d => {
        setSelectedCustomer(d.customer);
        setCustomerOrders(d.orders || []);
      });
  };

  const triggerSync = () => {
    setSyncing(true);
    setSyncError(null);
    fetch(`${API_BASE}/api/sync`)
      .then(r => r.json())
      .then(() => {
        // Poll for completion
        const poll = () => {
          fetch(`${API_BASE}/api/sync-status`)
            .then(r => r.json())
            .then(s => {
              if (s.done) {
                setSyncing(false);
                if (s.error) setSyncError(s.error);
                fetchDashboard();
              } else {
                setTimeout(poll, 5000);
              }
            });
        };
        setTimeout(poll, 5000);
      });
  };

  if (loading) return (
    <div className="min-h-screen bg-gray-50 flex flex-col items-center justify-center gap-4">
      <div className="text-2xl font-bold">Craft Dominant</div>
      <div className="text-gray-500">Loading dashboard...</div>
    </div>
  );

  const { portfolio, decisions, events, customers: cs } = dashboard || {};
  const isEmpty = !events?.length && !portfolio?.total_tickets;

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b sticky top-0 z-40">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Craft Dominant</h1>
            <p className="text-sm text-gray-500">{dashboard?.updated_at?.slice(0, 16)}</p>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex gap-2">
              {Object.entries(decisions || {}).map(([d, n]) => {
                const cfg = DECISIONS[d];
                return cfg && n > 0 ? (
                  <div key={d} className="flex items-center gap-1 px-3 py-1 rounded-full" style={{ backgroundColor: cfg.bg }}>
                    <span>{cfg.icon}</span>
                    <span className="font-bold" style={{ color: cfg.color }}>{n}</span>
                  </div>
                ) : null;
              })}
            </div>
            <div className="flex bg-gray-100 rounded-lg p-1">
              {['events', 'crm'].map(t => (
                <button key={t} onClick={() => setTab(t)} className={`px-4 py-2 rounded-lg font-medium ${tab === t ? 'bg-white shadow' : 'text-gray-600'}`}>
                  {t === 'events' ? 'Events' : 'CRM'}
                </button>
              ))}
            </div>
            <button onClick={() => { if (!syncing) triggerSync(); }} className={`px-3 py-2 rounded-lg text-sm font-medium ${syncing ? 'bg-gray-200 text-gray-400' : 'bg-blue-50 text-blue-600 hover:bg-blue-100'}`} disabled={syncing}>
              {syncing ? 'Syncing...' : 'Refresh Data'}
            </button>
          </div>
        </div>
      </header>

      {syncing && (
        <div className="bg-blue-600 text-white py-2 px-6 text-center text-sm">
          Syncing data from Eventbrite... This may take a few minutes on first load. Dashboard will update automatically.
        </div>
      )}

      {syncError && (
        <div className="bg-red-100 text-red-700 py-2 px-6 text-center text-sm">
          Sync error: {syncError}
        </div>
      )}

      <main className="max-w-7xl mx-auto px-6 py-6">
        {isEmpty && !syncing && (
          <Card className="p-8 text-center mb-6">
            <div className="text-xl font-bold text-gray-700 mb-2">No Data Yet</div>
            <p className="text-gray-500 mb-4">The dashboard needs to sync with Eventbrite to load your events and sales data.</p>
            <button onClick={triggerSync} className="px-6 py-2 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700">
              Sync Now
            </button>
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
                <Card className="p-6 text-center text-gray-500">
                  {syncing ? 'Loading events...' : 'No upcoming events found'}
                </Card>
              )}
            </div>
            <div className="col-span-8"><EventDetail event={selectedEvent} /></div>
          </div>
        )}

        {tab === 'crm' && (
          <div>
            <div className="grid grid-cols-6 gap-4 mb-6">
              {Object.entries(cs?.segments || {}).map(([s, n]) => {
                const cfg = SEGMENTS[s] || SEGMENTS.other;
                return (
                  <Card key={s} className="p-4 text-center">
                    <div className="text-2xl font-bold" style={{ color: cfg.color }}>{n}</div>
                    <div className="text-sm text-gray-500">{cfg.label}</div>
                  </Card>
                );
              })}
            </div>
            <Card><CustomerTable customers={customers} onSelect={handleSelectCustomer} /></Card>
          </div>
        )}
      </main>

      {decisions?.pivot > 0 && (
        <div className="fixed bottom-0 left-0 right-0 bg-red-600 text-white py-3 px-6 z-50">
          <div className="max-w-7xl mx-auto flex items-center justify-between">
            <span className="font-bold">ð¨ {decisions.pivot} event(s) require PIVOT</span>
            <button onClick={() => setTab('events')} className="px-4 py-1 bg-white text-red-600 rounded font-bold">View</button>
          </div>
        </div>
      )}

      {selectedCustomer && <CustomerModal customer={selectedCustomer} orders={customerOrders} onClose={() => setSelectedCustomer(null)} />}
    </div>
  );
}
