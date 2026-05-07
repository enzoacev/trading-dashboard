const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const WebSocket  = require('ws');
const path       = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

// ── Config — Bybit (sin restricciones geográficas) ───────────
const BYBIT_REST = 'https://api.bybit.com/v5/market';
const BYBIT_WS   = 'wss://stream.bybit.com/v5/public/linear';

const SYMBOLS = [
    { id: 'BTCUSDT',  name: 'Bitcoin',  symbol: 'BTC' },
    { id: 'ETHUSDT',  name: 'Ethereum', symbol: 'ETH' },
    { id: 'ADAUSDT',  name: 'Cardano',  symbol: 'ADA' },
    { id: 'SOLUSDT',  name: 'Solana',   symbol: 'SOL' },
    { id: 'XRPUSDT',  name: 'Ripple',   symbol: 'XRP' },
];

const candles = {};
const prices  = {};

// ── REST: cargar historial desde Bybit ───────────────────────
async function loadHistory(sym) {
    try {
        const url = `${BYBIT_REST}/kline?category=linear&symbol=${sym}&interval=60&limit=500`;
        const res  = await fetch(url);
        const json = await res.json();

        if (json.retCode !== 0) throw new Error(json.retMsg);

        // Bybit devuelve DESC, revertir a ASC
        const list = json.result.list.reverse();

        candles[sym] = list.map(k => ({
            t: parseInt(k[0]),
            o: parseFloat(k[1]),
            h: parseFloat(k[2]),
            l: parseFloat(k[3]),
            c: parseFloat(k[4]),
            v: parseFloat(k[5])
        }));

        prices[sym] = candles[sym][candles[sym].length - 1].c;
        console.log(`✅ Historial cargado: ${sym} (${candles[sym].length} velas)`);
    } catch (e) {
        console.error(`❌ Error cargando historial ${sym}:`, e.message);
    }
}

// ── WebSocket a Bybit ─────────────────────────────────────────
let wsPingInterval = null;

function openBybitStream() {
    const ws = new WebSocket(BYBIT_WS);

    ws.on('open', () => {
        console.log('🔌 WebSocket Bybit conectado');

        // Suscribir a klines de 1h
        const args = SYMBOLS.map(s => `kline.60.${s.id}`);
        ws.send(JSON.stringify({ op: 'subscribe', args }));

        // Bybit necesita ping cada 20s
        wsPingInterval = setInterval(() => {
            if (ws.readyState === ws.OPEN) {
                ws.send(JSON.stringify({ op: 'ping' }));
            }
        }, 20000);
    });

    ws.on('message', (raw) => {
        try {
            const msg = JSON.parse(raw);

            if (msg.op === 'pong' || msg.op === 'subscribe') return;
            if (!msg.data || !msg.topic) return;

            // topic: "kline.60.BTCUSDT"
            const sym    = msg.topic.split('.')[2];
            const klines = msg.data;
            if (!klines || !klines.length) return;

            const k      = klines[0];
            const closed = k.confirm;

            const candle = {
                t: k.start,
                o: parseFloat(k.open),
                h: parseFloat(k.high),
                l: parseFloat(k.low),
                c: parseFloat(k.close),
                v: parseFloat(k.volume)
            };

            prices[sym] = candle.c;
            if (!candles[sym]) candles[sym] = [];

            if (closed) {
                candles[sym].push(candle);
                if (candles[sym].length > 500) candles[sym].shift();
                console.log(`🕯  Vela cerrada ${sym}: $${candle.c.toFixed(2)}`);
            } else {
                candles[sym][candles[sym].length - 1] = candle;
            }

            const meta = SYMBOLS.find(s => s.id === sym);
            if (!meta) return;

            io.emit('update', {
                symbol:  sym,
                name:    meta.name,
                sym:     meta.symbol,
                price:   candle.c,
                closed,
                candles: candles[sym].slice(-200)
            });

        } catch (e) {
            console.error('Error procesando mensaje WS:', e.message);
        }
    });

    ws.on('close', () => {
        console.log('⚠️  WebSocket cerrado, reconectando en 5s...');
        if (wsPingInterval) clearInterval(wsPingInterval);
        setTimeout(openBybitStream, 5000);
    });

    ws.on('error', (e) => {
        console.error('❌ WebSocket error:', e.message);
    });
}

// ── Socket.IO ─────────────────────────────────────────────────
io.on('connection', (socket) => {
    console.log(`👤 Cliente conectado: ${socket.id}`);

    SYMBOLS.forEach(meta => {
        const sym = meta.id;
        if (!candles[sym] || !prices[sym]) return;

        socket.emit('update', {
            symbol:  sym,
            name:    meta.name,
            sym:     meta.symbol,
            price:   prices[sym],
            closed:  false,
            candles: candles[sym].slice(-200)
        });
    });

    socket.on('disconnect', () => {
        console.log(`👤 Cliente desconectado: ${socket.id}`);
    });
});

// ── Status endpoint ───────────────────────────────────────────
app.get('/api/status', (req, res) => {
    res.json({
        source: 'Bybit',
        symbols: SYMBOLS.map(s => ({
            name:    s.name,
            candles: candles[s.id]?.length || 0,
            price:   prices[s.id] || null
        }))
    });
});

// ── Startup ───────────────────────────────────────────────────
async function start() {
    console.log('🚀 Cargando historial desde Bybit...');

    for (const s of SYMBOLS) {
        await loadHistory(s.id);
        await new Promise(r => setTimeout(r, 300));
    }

    openBybitStream();

    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => {
        console.log(`✅ Servidor corriendo en puerto ${PORT}`);
    });
}

start();
