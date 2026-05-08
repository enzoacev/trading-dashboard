const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const WebSocket  = require('ws');
const path       = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

const BYBIT_REST = 'https://api.bybit.com/v5/market';
const BYBIT_WS   = 'wss://stream.bybit.com/v5/public/spot';

const SYMBOLS = [
    { id: 'BTCUSDT',  name: 'Bitcoin',  symbol: 'BTC' },
    { id: 'ETHUSDT',  name: 'Ethereum', symbol: 'ETH' },
    { id: 'ADAUSDT',  name: 'Cardano',  symbol: 'ADA' },
    { id: 'SOLUSDT',  name: 'Solana',   symbol: 'SOL' },
    { id: 'XRPUSDT',  name: 'Ripple',   symbol: 'XRP' },
];

const candles = {};
const prices  = {};

// ── Cargar historial REST ─────────────────────────────────────
async function loadHistory(sym) {
    try {
        // Usar spot para evitar restricciones de futuros
        const url = `${BYBIT_REST}/kline?category=spot&symbol=${sym}&interval=60&limit=500`;
        console.log(`📡 Cargando: ${url}`);
        
        // Agregar headers para evitar bloqueos de Cloudflare/WAF en Render
        const res = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json'
            }
        });

        // Si la respuesta no es 200 OK, lanzamos error mostrando el texto devuelto (generalmente HTML de bloqueo)
        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(`HTTP ${res.status} - ${errorText.substring(0, 100)}...`);
        }

        const json = await res.json();

        console.log(`📦 Respuesta ${sym}: retCode=${json.retCode}, items=${json.result?.list?.length}`);

        if (json.retCode !== 0) throw new Error(json.retMsg);

        const list = [...json.result.list].reverse();

        candles[sym] = list.map(k => ({
            t: parseInt(k[0]),
            o: parseFloat(k[1]),
            h: parseFloat(k[2]),
            l: parseFloat(k[3]),
            c: parseFloat(k[4]),
            v: parseFloat(k[5])
        }));

        prices[sym] = candles[sym][candles[sym].length - 1].c;
        console.log(`✅ ${sym}: ${candles[sym].length} velas, último precio: $${prices[sym]}`);
    } catch (e) {
        console.error(`❌ Error cargando historial ${sym}:`, e.message);
    }
}

// ── WebSocket Bybit spot ──────────────────────────────────────
let pingInterval = null;

function openStream() {
    const ws = new WebSocket(BYBIT_WS);

    ws.on('open', () => {
        console.log('🔌 WebSocket Bybit spot conectado');

        const args = SYMBOLS.map(s => `kline.60.${s.id}`);
        const msg  = JSON.stringify({ op: 'subscribe', args });
        console.log('📤 Suscribiendo:', msg);
        ws.send(msg);

        pingInterval = setInterval(() => {
            if (ws.readyState === ws.OPEN) {
                ws.send(JSON.stringify({ op: 'ping' }));
            }
        }, 20000);
    });

    ws.on('message', (raw) => {
        try {
            const msg = JSON.parse(raw.toString());

            // Log de cada mensaje para debug
            if (msg.op) {
                console.log(`📨 op=${msg.op}`, msg.success !== undefined ? `success=${msg.success}` : '');
                return;
            }

            if (!msg.topic || !msg.data) return;

            const parts = msg.topic.split('.');
            const sym   = parts[2];
            const data  = Array.isArray(msg.data) ? msg.data[0] : msg.data;

            const candle = {
                t: data.start   !== undefined ? data.start   : Date.now(),
                o: parseFloat(data.open  || data.o || 0),
                h: parseFloat(data.high  || data.h || 0),
                l: parseFloat(data.low   || data.l || 0),
                c: parseFloat(data.close || data.c || 0),
                v: parseFloat(data.volume|| data.v || 0)
            };

            if (!candle.c) return;

            const closed = data.confirm === true;
            prices[sym]  = candle.c;

            if (!candles[sym]) candles[sym] = [];

            if (closed) {
                candles[sym].push(candle);
                if (candles[sym].length > 500) candles[sym].shift();
                console.log(`🕯 Vela cerrada ${sym}: $${candle.c}`);
            } else {
                if (candles[sym].length > 0) {
                    candles[sym][candles[sym].length - 1] = candle;
                } else {
                    candles[sym].push(candle);
                }
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

    // Enviar datos actuales al cliente nuevo
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
        source: 'Bybit Spot',
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
        await new Promise(r => setTimeout(r, 500));
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