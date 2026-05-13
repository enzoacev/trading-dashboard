const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const WebSocket  = require('ws');
const path       = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

const KRAKEN_REST = 'https://api.kraken.com/0/public';
const KRAKEN_WS   = 'wss://ws.kraken.com';

const SYMBOLS = [
    { id: 'BTCUSDT', restPair: 'XBTUSD', wsPair: 'XBT/USD', name: 'Bitcoin',  symbol: 'BTC' },
    { id: 'ETHUSDT', restPair: 'ETHUSD', wsPair: 'ETH/USD', name: 'Ethereum', symbol: 'ETH' },
    { id: 'ADAUSDT', restPair: 'ADAUSD', wsPair: 'ADA/USD', name: 'Cardano',  symbol: 'ADA' },
    { id: 'SOLUSDT', restPair: 'SOLUSD', wsPair: 'SOL/USD', name: 'Solana',   symbol: 'SOL' },
    { id: 'XRPUSDT', restPair: 'XRPUSD', wsPair: 'XRP/USD', name: 'Ripple',   symbol: 'XRP' },
];

const wsPairToId = {};
SYMBOLS.forEach(s => { wsPairToId[s.wsPair] = s.id; });

const candles = {};
const prices  = {};

// ── Cargar historial REST ─────────────────────────────────────
async function loadHistory(sym) {
    try {
        const meta = SYMBOLS.find(s => s.id === sym);
        const url = `${KRAKEN_REST}/OHLC?pair=${meta.restPair}&interval=60`;
        console.log(`📡 Cargando: ${url}`);

        const res = await fetch(url);
        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(`HTTP ${res.status} - ${errorText.substring(0, 100)}...`);
        }

        const json = await res.json();
        if (json.error && json.error.length > 0) throw new Error(json.error[0]);

        // Kraken devuelve el resultado bajo la clave del par (puede variar, ej: XXBTZUSD)
        const resultKey = Object.keys(json.result).find(k => k !== 'last');
        const list = json.result[resultKey];

        console.log(`📦 Respuesta ${sym}: items=${list?.length}`);

        // Kraken OHLC: [time, open, high, low, close, vwap, volume, count]
        candles[sym] = list.slice(-500).map(k => ({
            t: parseInt(k[0]) * 1000,
            o: parseFloat(k[1]),
            h: parseFloat(k[2]),
            l: parseFloat(k[3]),
            c: parseFloat(k[4]),
            v: parseFloat(k[6])
        }));

        prices[sym] = candles[sym][candles[sym].length - 1].c;
        console.log(`✅ ${sym}: ${candles[sym].length} velas, último precio: $${prices[sym]}`);
    } catch (e) {
        console.error(`❌ Error cargando historial ${sym}:`, e.message);
    }
}

// ── WebSocket Kraken ──────────────────────────────────────────
let pingInterval = null;

function openStream() {
    const ws = new WebSocket(KRAKEN_WS);

    ws.on('open', () => {
        console.log('🔌 WebSocket Kraken conectado');

        const pairs = SYMBOLS.map(s => s.wsPair);
        const msg = JSON.stringify({
            event: 'subscribe',
            pair: pairs,
            subscription: { name: 'ohlc', interval: 60 }
        });
        console.log('📤 Suscribiendo:', msg);
        ws.send(msg);

        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ event: 'ping' }));
            }
        }, 20000);
    });

    ws.on('message', (raw) => {
        try {
            const msg = JSON.parse(raw.toString());

            // Kraken envía eventos como objetos
            if (msg.event) {
                console.log(`📨 event=${msg.event}`);
                return;
            }

            // Actualización OHLC: [channelID, [time, etime, open, high, low, close, vwap, volume, count], "ohlc-60", "XBT/USD"]
            if (!Array.isArray(msg) || msg.length < 4) return;
            const wsPair = msg[3];
            const sym    = wsPairToId[wsPair];
            if (!sym) return;

            const data = msg[1];
            const candle = {
                t: parseFloat(data[0]) * 1000,
                o: parseFloat(data[2]),
                h: parseFloat(data[3]),
                l: parseFloat(data[4]),
                c: parseFloat(data[5]),
                v: parseFloat(data[7])
            };

            if (!candle.c) return;

            prices[sym] = candle.c;
            if (!candles[sym]) candles[sym] = [];

            const last = candles[sym][candles[sym].length - 1];
            if (last && last.t === candle.t) {
                candles[sym][candles[sym].length - 1] = candle;
            } else {
                candles[sym].push(candle);
                if (candles[sym].length > 500) candles[sym].shift();
                console.log(`🕯 Nueva vela ${sym}: $${candle.c}`);
            }

            const meta = SYMBOLS.find(s => s.id === sym);
            if (!meta) return;

            io.emit('update', {
                symbol:  sym,
                name:    meta.name,
                sym:     meta.symbol,
                price:   candle.c,
                closed:  false,
                candles: candles[sym].slice(-200)
            });

        } catch (e) {
            console.error('❌ Error procesando WS:', e.message, raw.toString().slice(0, 200));
        }
    });

    ws.on('close', (code, reason) => {
        console.log(`⚠️  WS cerrado: code=${code} reason=${reason} — reconectando en 5s`);
        if (pingInterval) clearInterval(pingInterval);
        setTimeout(openStream, 5000);
    });

    ws.on('error', (e) => {
        console.error('❌ WS error:', e.message);
    });
}

// ── Socket.IO ─────────────────────────────────────────────────
io.on('connection', (socket) => {
    console.log(`👤 Cliente conectado: ${socket.id}`);

    let sent = 0;
    SYMBOLS.forEach(meta => {
        const sym = meta.id;
        if (!candles[sym]?.length || !prices[sym]) {
            console.log(`⚠️  Sin datos para ${sym} al conectar cliente`);
            return;
        }

        socket.emit('update', {
            symbol:  sym,
            name:    meta.name,
            sym:     meta.symbol,
            price:   prices[sym],
            closed:  false,
            candles: candles[sym].slice(-200)
        });
        sent++;
    });

    console.log(`📤 Enviados ${sent}/${SYMBOLS.length} símbolos al cliente ${socket.id}`);

    socket.on('disconnect', () => {
        console.log(`👤 Cliente desconectado: ${socket.id}`);
    });
});

// ── Status API ────────────────────────────────────────────────
app.get('/api/status', (req, res) => {
    res.json({
        source: 'Kraken',
        uptime: process.uptime(),
        symbols: SYMBOLS.map(s => ({
            id:      s.id,
            name:    s.name,
            candles: candles[s.id]?.length || 0,
            price:   prices[s.id] || null,
            hasData: (candles[s.id]?.length || 0) > 0
        }))
    });
});

// ── Startup ───────────────────────────────────────────────────
async function start() {
    console.log('🚀 Iniciando servidor...');

    for (const s of SYMBOLS) {
        await loadHistory(s.id);
        await new Promise(r => setTimeout(r, 300));
    }

    const loaded = SYMBOLS.filter(s => candles[s.id]?.length > 0).length;
    console.log(`📊 Historial: ${loaded}/${SYMBOLS.length} símbolos cargados`);

    openStream();

    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => {
        console.log(`✅ Servidor en puerto ${PORT}`);
        console.log(`🔗 Status: http://localhost:${PORT}/api/status`);
    });
}

start();
