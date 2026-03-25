import React, { useState, useEffect, useCallback } from 'react';

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || 'https://craft-dominant-production.up.railway.app';

const STATUS_CONFIG = {
  draft:    { color: '#6b7280', bg: '#f3f4f6', label: 'DRAFT', icon: '📝' },
  queued:   { color: '#d97706', bg: '#fffbeb', label: 'QUEUED', icon: '📋' },
  approved: { color: '#16a34a', bg: '#f0fdf4', label: 'APPROVED', icon: '✅' },
  sending:  { color: '#2563eb', bg: '#eff6ff', label: 'SENDING', icon: '📨' },
  sent:     { color: '#7c3aed', bg: '#f5f3ff', label: 'SENT', icon: '✉️' },
  rejected: { color: '#dc2626', bg: '#fef2f2', label: 'REJECTED', icon: '❌' },
  error:    { color: '#dc2626', bg: '#fef2f2', label: 'ERROR', icon: '⚠️' },
};

const BARRIER_LABELS = {
  availability: '📅 Availability',
  social: '👥 Social',
  concept: '💡 Concept',
  value: '💰 Value',
  urgency: '⏰ Urgency',
};

const Badge = ({ status }) => {
  const cfg = STATUS_CONFIG[status] || STATUS_CONFIG.draft;
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: '4px',
      padding: '4px 10px', borderRadius: '20px', fontSize: '12px', fontWeight: '600',
      background: cfg.bg, color: cfg.color,
    }}>
      {cfg.icon} {cfg.label}
    </span>
  );
};

const CampaignCard = ({ campaign, onApprove, onReject, onSend, onDryRun, onSelect, selected }) => {
  const isSelected = selected?.id === campaign.id;
  return (
    <div
      onClick={() => onSelect(campaign)}
      style={{
        background: '#fff', borderRadius: '12px', padding: '16px',
        border: `2px solid ${isSelected ? '#2563eb' : '#eee'}`,
        cursor: 'pointer', transition: 'all 0.2s',
        boxShadow: isSelected ? '0 4px 12px rgba(37,99,235,0.15)' : '0 1px 3px rgba(0,0,0,0.05)',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '8px' }}>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: '15px', fontWeight: '600', color: '#1a1a2e', marginBottom: '4px' }}>
            {campaign.subject_line}
          </div>
          <div style={{ fontSize: '12px', color: '#999' }}>
            {campaign.campaign_type} &bull; {campaign.segment_name || 'No segment'} &bull; {campaign.audience_count?.toLocaleString()} recipients
          </div>
        </div>
        <Badge status={campaign.status} />
      </div>

      {campaign.barrier_addressed && (
        <div style={{ fontSize: '12px', color: '#666', marginBottom: '8px' }}>
          Barrier: {BARRIER_LABELS[campaign.barrier_addressed] || campaign.barrier_addressed}
          {campaign.confidence_score > 0 && ` • ${(campaign.confidence_score * 100).toFixed(0)}% confidence`}
        </div>
      )}

      {campaign.predicted_revenue > 0 && (
        <div style={{ fontSize: '13px', color: '#16a34a', fontWeight: '500', marginBottom: '8px' }}>
          Predicted: ${campaign.predicted_revenue.toLocaleString()}
        </div>
      )}

      {campaign.status === 'sent' && campaign.sends > 0 && (
        <div style={{ display: 'flex', gap: '16px', fontSize: '13px', color: '#555', marginBottom: '8px' }}>
          <span>{campaign.sends?.toLocaleString()} sent</span>
          <span>{campaign.opens?.toLocaleString()} opens ({campaign.sends > 0 ? (campaign.opens * 100 / campaign.sends).toFixed(1) : 0}%)</span>
          <span>{campaign.clicks?.toLocaleString()} clicks ({campaign.sends > 0 ? (campaign.clicks * 100 / campaign.sends).toFixed(1) : 0}%)</span>
        </div>
      )}

      {/* Action buttons for actionable states */}
      {(campaign.status === 'draft' || campaign.status === 'queued') && (
        <div style={{ display: 'flex', gap: '8px', marginTop: '8px' }}>
          <button
            onClick={(e) => { e.stopPropagation(); onApprove(campaign.id); }}
            style={{
              padding: '8px 16px', borderRadius: '8px', border: 'none',
              background: '#16a34a', color: '#fff', fontWeight: '600',
              fontSize: '13px', cursor: 'pointer',
            }}
          >
            ✅ Approve
          </button>
          <button
            onClick={(e) => { e.stopPropagation(); onReject(campaign.id); }}
            style={{
              padding: '8px 16px', borderRadius: '8px', border: '2px solid #e5e7eb',
              background: 'transparent', color: '#6b7280', fontWeight: '600',
              fontSize: '13px', cursor: 'pointer',
            }}
          >
            Reject
          </button>
        </div>
      )}

      {campaign.status === 'approved' && (
        <div style={{ display: 'flex', gap: '8px', marginTop: '8px' }}>
          <button
            onClick={(e) => { e.stopPropagation(); onSend(campaign.id); }}
            style={{
              padding: '8px 20px', borderRadius: '8px', border: 'none',
              background: '#2563eb', color: '#fff', fontWeight: '600',
              fontSize: '13px', cursor: 'pointer',
            }}
          >
            📨 Send Now
          </button>
          <button
            onClick={(e) => { e.stopPropagation(); onDryRun(campaign.id); }}
            style={{
              padding: '8px 16px', borderRadius: '8px', border: '2px solid #e5e7eb',
              background: 'transparent', color: '#6b7280', fontWeight: '600',
              fontSize: '13px', cursor: 'pointer',
            }}
          >
            🔍 Dry Run
          </button>
        </div>
      )}
    </div>
  );
};

const CampaignDetail = ({ campaign }) => {
  if (!campaign) return (
    <div style={{ background: '#fff', borderRadius: '12px', padding: '40px', textAlign: 'center', color: '#999', border: '2px solid #eee' }}>
      Select a campaign to preview
    </div>
  );

  return (
    <div style={{ background: '#fff', borderRadius: '12px', border: '2px solid #eee', overflow: 'hidden' }}>
      {/* Header */}
      <div style={{ padding: '20px', borderBottom: '1px solid #eee' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
          <h2 style={{ fontSize: '18px', fontWeight: '700', color: '#1a1a2e' }}>Campaign Preview</h2>
          <Badge status={campaign.status} />
        </div>
        <div style={{ fontSize: '13px', color: '#666' }}>
          {campaign.campaign_type} &bull; Created {new Date(campaign.created_at).toLocaleDateString()}
          {campaign.scheduled_send_at && ` • Scheduled: ${new Date(campaign.scheduled_send_at).toLocaleString()}`}
        </div>
      </div>

      {/* Strategic reasoning */}
      {campaign.strategic_reasoning && (
        <div style={{ padding: '16px 20px', background: '#f8f9fa', borderBottom: '1px solid #eee' }}>
          <div style={{ fontSize: '12px', fontWeight: '600', color: '#999', marginBottom: '4px', textTransform: 'uppercase' }}>AI Reasoning</div>
          <div style={{ fontSize: '14px', color: '#333', lineHeight: '1.5' }}>{campaign.strategic_reasoning}</div>
        </div>
      )}

      {/* Email preview */}
      <div style={{ padding: '20px' }}>
        <div style={{ fontSize: '12px', fontWeight: '600', color: '#999', marginBottom: '4px', textTransform: 'uppercase' }}>Subject Line</div>
        <div style={{ fontSize: '16px', fontWeight: '600', color: '#1a1a2e', marginBottom: '16px' }}>{campaign.subject_line}</div>

        {campaign.preview_text && (
          <>
            <div style={{ fontSize: '12px', fontWeight: '600', color: '#999', marginBottom: '4px', textTransform: 'uppercase' }}>Preview Text</div>
            <div style={{ fontSize: '14px', color: '#666', marginBottom: '16px' }}>{campaign.preview_text}</div>
          </>
        )}

        <div style={{ fontSize: '12px', fontWeight: '600', color: '#999', marginBottom: '8px', textTransform: 'uppercase' }}>Email Body</div>
        <div style={{
          border: '1px solid #e5e7eb', borderRadius: '8px', overflow: 'hidden',
          maxHeight: '500px', overflowY: 'auto',
        }}>
          <div dangerouslySetInnerHTML={{ __html: campaign.body_html }} />
        </div>
      </div>

      {/* Segment info */}
      {campaign.segment_sql && (
        <div style={{ padding: '16px 20px', background: '#f8f9fa', borderTop: '1px solid #eee' }}>
          <div style={{ fontSize: '12px', fontWeight: '600', color: '#999', marginBottom: '4px', textTransform: 'uppercase' }}>Segment Query</div>
          <code style={{ fontSize: '12px', color: '#555', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{campaign.segment_sql}</code>
        </div>
      )}
    </div>
  );
};

export default function CampaignsPage() {
  const [campaigns, setCampaigns] = useState([]);
  const [selected, setSelected] = useState(null);
  const [filter, setFilter] = useState('all');
  const [loading, setLoading] = useState(true);
  const [engineStatus, setEngineStatus] = useState(null);
  const [generating, setGenerating] = useState(false);
  const [dryRunResult, setDryRunResult] = useState(null);

  const fetchCampaigns = useCallback(() => {
    const url = filter === 'all' ? `${API_BASE}/api/campaigns` : `${API_BASE}/api/campaigns?status=${filter}`;
    fetch(url)
      .then(r => r.json())
      .then(data => { setCampaigns(Array.isArray(data) ? data : []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [filter]);

  useEffect(() => { fetchCampaigns(); }, [fetchCampaigns]);

  // Fetch engine status on load
  useEffect(() => {
    fetch(`${API_BASE}/api/engine/status`)
      .then(r => r.json())
      .then(setEngineStatus)
      .catch(() => {});
  }, []);

  const handleApprove = async (id) => {
    await fetch(`${API_BASE}/api/campaigns/${id}/approve`, { method: 'POST' });
    fetchCampaigns();
  };

  const handleReject = async (id) => {
    await fetch(`${API_BASE}/api/campaigns/${id}/reject`, { method: 'POST' });
    fetchCampaigns();
  };

  const handleSend = async (id) => {
    if (!confirm('Send this campaign now? This will email all recipients in the segment.')) return;
    await fetch(`${API_BASE}/api/campaigns/${id}/send`, { method: 'POST' });
    fetchCampaigns();
  };

  const handleDryRun = async (id) => {
    try {
      const resp = await fetch(`${API_BASE}/api/campaigns/${id}/dry-run`, { method: 'POST' });
      const data = await resp.json();
      setDryRunResult(data);
    } catch (err) {
      alert('Dry run failed: ' + err.message);
    }
  };

  const handleGenerate = async () => {
    setGenerating(true);
    try {
      const resp = await fetch(`${API_BASE}/api/campaigns/generate`, { method: 'POST' });
      const data = await resp.json();
      alert(`Generated ${data.generated || 0} campaigns`);
      fetchCampaigns();
    } catch (err) {
      alert('Generation failed: ' + err.message);
    }
    setGenerating(false);
  };

  const pendingCount = campaigns.filter(c => c.status === 'draft' || c.status === 'queued').length;

  return (
    <div style={{
      minHeight: '100vh', background: '#f5f6f8', padding: '24px',
      fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
    }}>
      <div style={{ maxWidth: '1400px', margin: '0 auto' }}>
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
          <div>
            <h1 style={{ fontSize: '24px', fontWeight: '700', color: '#1a1a2e' }}>Campaign Queue</h1>
            <p style={{ color: '#666', fontSize: '14px' }}>
              {pendingCount > 0
                ? `${pendingCount} campaign${pendingCount !== 1 ? 's' : ''} waiting for approval`
                : 'All caught up'}
            </p>
          </div>
          <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
            {engineStatus && (
              <div style={{ fontSize: '12px', color: '#666', textAlign: 'right', marginRight: '8px' }}>
                <span style={{ color: engineStatus.claude_configured ? '#16a34a' : '#dc2626' }}>
                  {engineStatus.claude_configured ? '●' : '○'} Claude
                </span>
                {' · '}
                <span style={{ color: engineStatus.sendgrid_configured ? '#16a34a' : '#dc2626' }}>
                  {engineStatus.sendgrid_configured ? '●' : '○'} SendGrid
                </span>
              </div>
            )}
            <button
              onClick={handleGenerate}
              disabled={generating}
              style={{
                padding: '10px 20px', borderRadius: '8px', border: 'none',
                background: '#16a34a', color: '#fff', fontWeight: '600', fontSize: '14px',
                cursor: generating ? 'wait' : 'pointer', opacity: generating ? 0.7 : 1,
              }}
            >
              {generating ? '⏳ Generating...' : '🤖 Generate Campaigns'}
            </button>
            <a href="/" style={{
              padding: '10px 20px', borderRadius: '8px', background: '#1a1a2e', color: '#fff',
              textDecoration: 'none', fontWeight: '600', fontSize: '14px',
            }}>
              ← Dashboard
            </a>
          </div>
        </div>

        {/* Filter tabs */}
        <div style={{ display: 'flex', gap: '8px', marginBottom: '20px' }}>
          {['all', 'draft', 'approved', 'sent', 'rejected'].map(f => (
            <button key={f} onClick={() => setFilter(f)}
              style={{
                padding: '8px 16px', borderRadius: '8px', border: '2px solid',
                borderColor: filter === f ? '#2563eb' : '#e5e7eb',
                background: filter === f ? '#eff6ff' : '#fff',
                color: filter === f ? '#2563eb' : '#666',
                fontWeight: '600', fontSize: '13px', cursor: 'pointer',
                textTransform: 'capitalize',
              }}>
              {f}
            </button>
          ))}
        </div>

        {/* Main layout */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>
          {/* Campaign list */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {loading && <div style={{ textAlign: 'center', padding: '40px', color: '#999' }}>Loading campaigns...</div>}
            {!loading && campaigns.length === 0 && (
              <div style={{ textAlign: 'center', padding: '40px', color: '#999', background: '#fff', borderRadius: '12px' }}>
                No campaigns{filter !== 'all' ? ` with status "${filter}"` : ''}. The system will generate campaigns as events approach phase transitions.
              </div>
            )}
            {campaigns.map(c => (
              <CampaignCard
                key={c.id}
                campaign={c}
                selected={selected}
                onSelect={setSelected}
                onApprove={handleApprove}
                onReject={handleReject}
                onSend={handleSend}
                onDryRun={handleDryRun}
              />
            ))}
          </div>

          {/* Detail panel */}
          <div style={{ position: 'sticky', top: '24px', alignSelf: 'start' }}>
            {dryRunResult && (
              <div style={{
                background: '#fffbeb', border: '2px solid #d97706', borderRadius: '12px',
                padding: '16px', marginBottom: '12px',
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                  <div style={{ fontSize: '14px', fontWeight: '700', color: '#92400e' }}>🔍 Dry Run Result</div>
                  <button onClick={() => setDryRunResult(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#999' }}>✕</button>
                </div>
                {dryRunResult.error ? (
                  <div style={{ color: '#dc2626', fontSize: '13px' }}>{dryRunResult.error}</div>
                ) : (
                  <>
                    <div style={{ fontSize: '13px', color: '#555', marginBottom: '4px' }}>
                      <strong>{dryRunResult.audience_count?.toLocaleString()}</strong> recipients would receive this email
                    </div>
                    <div style={{ fontSize: '12px', color: '#666', marginBottom: '4px' }}>
                      Subject: {dryRunResult.subject_line}
                    </div>
                    {dryRunResult.sample_recipients?.length > 0 && (
                      <div style={{ fontSize: '11px', color: '#888', marginTop: '8px' }}>
                        <div style={{ fontWeight: '600', marginBottom: '2px' }}>Sample recipients:</div>
                        {dryRunResult.sample_recipients.map((e, i) => (
                          <div key={i}>{e}</div>
                        ))}
                        {dryRunResult.audience_count > 10 && <div>...and {dryRunResult.audience_count - 10} more</div>}
                      </div>
                    )}
                  </>
                )}
              </div>
            )}
            <CampaignDetail campaign={selected} />
          </div>
        </div>
      </div>
    </div>
  );
}
