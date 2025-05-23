const express = require('express');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());

const PORT = process.env.PORT || 3000;

// Store prices in memory
const prices = {
  binance: {},
  mexc: {}
};

// WebSocket connections
const binanceSockets = {};
const mexcSockets = {};

// Binance WebSocket handler
function setupBinanceWS(symbol) {
  const wsUrl = `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@aggTrade`;
  const ws = new WebSocket(wsUrl);

  ws.on('open', () => {
    console.log(`✅ Binance WS connected for ${symbol}`);
    binanceSockets[symbol] = ws;
  });

  ws.on('message', (data) => {
    try {
      const trade = JSON.parse(data);
      prices.binance[symbol] = parseFloat(trade.p);
    } catch (e) {
      console.error(`⚠️ Binance WS error for ${symbol}:`, e.message);
    }
  });

  ws.on('close', () => {
    console.log(`❌ Binance WS closed for ${symbol}`);
    delete binanceSockets[symbol];
    setTimeout(() => setupBinanceWS(symbol), 5000);
  });

  ws.on('error', (err) => {
    console.error(`⚠️ Binance WS error for ${symbol}:`, err.message);
  });
}

// MEXC WebSocket handler with ping
const MEXC_WS_URL = "wss://wbs.mexc.com/ws";

function setupMexcWS(symbol) {
  const ws = new WebSocket(MEXC_WS_URL);
  let pingInterval;
  let reconnectAttempts = 0;
  const MAX_RECONNECT_ATTEMPTS = 5;
  const RECONNECT_DELAY = 3000;

  const sendPing = () => {
    if (ws.readyState === ws.OPEN) {
      ws.ping();
    }
  };

  ws.on('open', () => {
    console.log(`✅ MEXC Futures WS connected for ${symbol}`);
    mexcSockets[symbol] = ws;
    reconnectAttempts = 0;

    pingInterval = setInterval(sendPing, 30000);

    ws.send(JSON.stringify({
      method: "sub.deal",
      params: [symbol],
      id: Date.now()
    }));
  });

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);
      
      if (message === 'pong') return;
      
      if (message.channel === "push.deal" && message.data) {
        const latestTrade = message.data.deals[0];
        if (latestTrade && latestTrade.p) {
          prices.mexc[symbol] = parseFloat(latestTrade.p);
        }
      }
    } catch (e) {
      console.error(`⚠️ MEXC WS parse error for ${symbol}:`, e.message);
    }
  });

  ws.on('pong', () => {
    console.log(`🏓 MEXC WS pong received for ${symbol}`);
  });

  ws.on('close', () => {
    console.log(`❌ MEXC WS closed for ${symbol}`);
    clearInterval(pingInterval);
    delete mexcSockets[symbol];
    
    if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
      reconnectAttempts++;
      const delay = RECONNECT_DELAY * reconnectAttempts;
      console.log(`🔁 Reconnecting to MEXC for ${symbol} in ${delay / 1000} seconds...`);
      setTimeout(() => setupMexcWS(symbol), delay);
    } else {
      console.error(`⛔ Max reconnection attempts (${MAX_RECONNECT_ATTEMPTS}) reached for ${symbol}`);
    }
  });

  ws.on('error', (err) => {
    console.error(`⚠️ MEXC WS error for ${symbol}:`, err.message);
    ws.close();
  });
}

// Initialize WebSockets
const DEFAULT_SYMBOLS = {
  binance: ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt'],
  mexc: ['BTC_USDT', 'ETH_USDT', 'SOL_USDT', 'XRP_USDT']
};

DEFAULT_SYMBOLS.binance.forEach(symbol => setupBinanceWS(symbol));
DEFAULT_SYMBOLS.mexc.forEach(symbol => setupMexcWS(symbol));

// ✅ Added root route for Render default check
app.get('/', (req, res) => {
  res.send('✅ Node.js backend with MEXC/Binance WS is running on Render!');
});

// REST API endpoints
app.get('/api/prices', (req, res) => {
  res.json(prices);
});

app.get('/api/price-difference', (req, res) => {
  const { binanceSymbol, mexcSymbol } = req.query;

  if (!binanceSymbol || !mexcSymbol) {
    return res.status(400).json({ error: 'Both symbols are required' });
  }

  const binancePrice = prices.binance[binanceSymbol.toLowerCase()];
  const mexcPrice = prices.mexc[mexcSymbol.toUpperCase()];

  if (binancePrice === undefined || mexcPrice === undefined) {
    return res.status(404).json({ error: 'Price data not available for one or both symbols' });
  }

  const diff = binancePrice - mexcPrice;
  const percentDiff = (diff / ((binancePrice + mexcPrice) / 2)) * 100;

  res.json({
    binancePrice,
    mexcPrice,
    difference: diff,
    percentDifference: percentDiff
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
