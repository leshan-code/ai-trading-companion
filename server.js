// AI Trading Companion — webhook + SSE server
// Receives TradingView alert JSON on POST /webhook, keeps the latest state
// per symbol in memory, and streams every update to connected dashboards
// over Server-Sent Events (GET /events).

const express = require('express');
const path = require('path');

const app = express();
app.use(express.json({ limit: '256kb' }));
app.use(express.static(path.join(__dirname, 'public')));

const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || ''; // set this on your host; leave blank only for local testing

// latest known state per symbol, and a rolling event log for the journal
const latestBySymbol = {};
const eventLog = [];
const MAX_LOG = 500;

// connected SSE clients
const clients = new Set();

function broadcast(event) {
  const payload = `data: ${JSON.stringify(event)}\n\n`;
  for (const res of clients) {
    res.write(payload);
  }
}

app.post('/webhook', (req, res) => {
  if (WEBHOOK_TOKEN) {
    const token = req.query.token || req.get('X-Webhook-Token');
    if (token !== WEBHOOK_TOKEN) {
      return res.status(401).json({ ok: false, error: 'bad token' });
    }
  }

  const body = req.body || {};
  if (!body.symbol || !body.state) {
    return res.status(400).json({ ok: false, error: 'expected JSON with at least symbol and state' });
  }

  const event = { ...body, receivedAt: new Date().toISOString() };
  latestBySymbol[body.symbol] = event;
  eventLog.push(event);
  if (eventLog.length > MAX_LOG) eventLog.shift();

  broadcast({ type: 'update', event });
  res.json({ ok: true });
});

app.get('/events', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.flushHeaders();

  // send current snapshot immediately so a fresh dashboard load isn't empty
  res.write(`data: ${JSON.stringify({ type: 'snapshot', latestBySymbol, eventLog })}\n\n`);

  clients.add(res);
  const heartbeat = setInterval(() => res.write(': heartbeat\n\n'), 20000);

  req.on('close', () => {
    clearInterval(heartbeat);
    clients.delete(res);
  });
});

app.get('/health', (req, res) => res.json({ ok: true, symbols: Object.keys(latestBySymbol) }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`AI Trading Companion server listening on :${PORT}`));
