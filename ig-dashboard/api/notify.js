// Email notification handler
// Sends Gmail notifications for DCA executions, rule triggers, and alerts
const nodemailer = require('nodemailer');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { type, subject, body, to } = req.body || {};

  if (!type || !body) {
    return res.status(400).json({ error: 'Missing type or body' });
  }

  // Gmail SMTP config from environment variables
  const gmailUser = process.env.GMAIL_USER;
  const gmailPass = process.env.GMAIL_APP_PASSWORD;
  const recipient = to || process.env.NOTIFY_EMAIL || gmailUser;

  if (!gmailUser || !gmailPass) {
    console.warn('[Notify] Gmail credentials not configured');
    return res.status(200).json({ 
      sent: false, 
      reason: 'Gmail not configured — set GMAIL_USER and GMAIL_APP_PASSWORD in Vercel env vars' 
    });
  }

  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: gmailUser,
      pass: gmailPass
    }
  });

  const emailSubject = subject || getDefaultSubject(type);
  const htmlBody = buildEmailHTML(type, body);

  try {
    await transporter.sendMail({
      from: `"IG Investment Programme" <${gmailUser}>`,
      to: recipient,
      subject: emailSubject,
      html: htmlBody,
      text: body
    });

    console.log('[Notify] Email sent:', emailSubject, '→', recipient);
    res.status(200).json({ sent: true, to: recipient, subject: emailSubject });
  } catch (err) {
    console.error('[Notify] Email error:', err.message);
    res.status(500).json({ sent: false, error: err.message });
  }
};

function getDefaultSubject(type) {
  const subjects = {
    dca: '📈 DCA Order Executed — IG Investment Programme',
    rule: '⚡ Rule Triggered — IG Investment Programme',
    error: '⚠️ Error Alert — IG Investment Programme',
    rebalance: '⚖️ Portfolio Rebalanced — IG Investment Programme',
    position_closed: '🔴 Position Closed — IG Investment Programme',
    daily_summary: '📊 Daily Summary — IG Investment Programme'
  };
  return subjects[type] || '🔔 Alert — IG Investment Programme';
}

function buildEmailHTML(type, body) {
  const colors = {
    dca: '#0072CE',
    rule: '#EF9F27',
    error: '#E24B4A',
    rebalance: '#1D9E75',
    position_closed: '#E24B4A',
    daily_summary: '#0072CE'
  };
  const color = colors[type] || '#0072CE';

  return `
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:560px;margin:40px auto;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
    <div style="background:${color};padding:24px 32px;">
      <div style="color:#fff;font-size:12px;font-weight:600;letter-spacing:1px;text-transform:uppercase;opacity:0.8">IG Investment Programme</div>
      <div style="color:#fff;font-size:22px;font-weight:500;margin-top:6px">${getDefaultSubject(type).replace(/^[^\s]+ /, '')}</div>
    </div>
    <div style="padding:32px;color:#1a1a18;font-size:14px;line-height:1.7;">
      ${body.split('\n').map(line => 
        line.trim() ? `<p style="margin:0 0 12px">${line}</p>` : '<br>'
      ).join('')}
    </div>
    <div style="padding:16px 32px;background:#f5f5f3;border-top:1px solid #eeece8;font-size:11px;color:#9b9b96;">
      Sent by your IG Automated Investment Programme &bull; ${new Date().toLocaleString('en-GB', {timeZone:'Europe/London'})}
    </div>
  </div>
</body>
</html>`;
}
