const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const WebSocket  = require('ws');
const path       = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

// ── Servir el HTML ────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ── Config ────────────────────────────────────────────────────
const BINANCE_REST = 'https://api.binance.com/api/v3';
const BINANCE_WS   = 'wss://stream.binance.com:9443/stream';

const SYMBOLS = [
    { binance: 'BTCUSDT',  name: 'Bitcoin',  symbol: 'BTC' },
    { binance: 'ETHUSDT',  name: 'Ethereum', symbol: 'ETH' },
    { binance: 'ADAUSDT',  name: 'Cardano',  symbol: 'ADA' },
    { binance: 'SOLUSDT',  name: 'Solana',   symbol: 'SOL' },
    { binance: 'XRPUSDT',  name: 'Ripple',   symbol: 'XRP' },
];

// ── Cache de velas históricas OHLCV ──────────────────────────
// Guardamos 500 velas de 1h por símbolo para calcular indicadores
const candles = {};   // { BTCUSDT: [ {o,h,l,c,v,t}, ... ] }
const prices  = {};   // { BTCUSDT: currentPrice }

// ── REST: cargar historial inicial ───────────────────────────
async function loadHistory(symbol) {
    try {
        const url = `${BINANCE_REST}/klines?symbol=${symbol}&interval=1h&limit=500`;
        const res  = await fetch(url);
        const data = await res.json();

        candles[symbol] = data.map(k => ({
            t: k[0],          // open time
            o: parseFloat(k[1]),  // open
            h: parseFloat(k[2]),  // high
            l: parseFloat(k[3]),  // low
            c: parseFloat(k[4]),  // close
            v: parseFloat(k[5])   // volume
        }));

        prices[symbol] = candles[symbol][candles[symbol].length - 1].c;
        console.log(`✅ Historial cargado: ${symbol} (${candles[symbol].length} velas)`);
    } catch (e) {
        console.error(`❌ Error cargando historial ${symbol}:`, e.message);
    }
}

// ── WebSocket a Binance ───────────────────────────────────────
// Suscribimos al stream combinado de klines 1h de todos los símbolos
function openBinanceStream() {
    const streams = SYMBOLS
        .map(s => `${s.binance.toLowerCase()}@kline_1h`)
        .join('/');

    const wsUrl = `${BINANCE_WS}?streams=${streams}`;
    const ws    = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log('🔌 WebSocket Binance conectado');
    });

    ws.on('message', (raw) => {
        try {
            const msg    = JSON.parse(raw);
            const kline  = msg.data?.k;
            if (!kline) return;

            const sym = kline.s;           // ej: BTCUSDT
            const closed = kline.x;        // true = vela cerrada
            const candle = {
                t: kline.t,
                o: parseFloat(kline.o),
                h: parseFloat(kline.h),
                l: parseFloat(kline.l),
                c: parseFloat(kline.c),
                v: parseFloat(kline.v)
            };

            // Actualizar precio actual siempre (tick a tick)
            prices[sym] = candle.c;

            if (!candles[sym]) candles[sym] = [];

            if (closed) {
                // Vela cerrada: agregar al historial y mantener últimas 500
                candles[sym].push(candle);
                if (candles[sym].length > 500) candles[sym].shift();
                console.log(`🕯  Vela cerrada ${sym}: $${candle.c.toFixed(2)}`);
            } else {
                // Vela en curso: actualizar la última
                if (candles[sym].length > 0) {
                    candles[sym][candles[sym].length - 1] = candle;
                }
            }

            // Emitir datos actualizados a todos los clientes conectados
            const meta = SYMBOLS.find(s => s.binance === sym);
            if (!meta) return;

            io.emit('update', {
                symbol:   sym,
                name:     meta.name,
                sym:      meta.symbol,
                price:    candle.c,
                closed,
                candles:  candles[sym].slice(-200)   // enviamos últimas 200 velas
            });

        } catch (e) {
            console.error('Error procesando mensaje WS:', e.message);
        }
    });

    ws.on('close', () => {
        console.log('⚠️  WebSocket cerrado, reconectando en 5s...');
        setTimeout(openBinanceStream, 5000);
    });

    ws.on('error', (e) => {
        console.error('❌ WebSocket error:', e.message);
    });
}

// ── Socket.IO: cuando conecta un cliente ─────────────────────
io.on('connection', (socket) => {
    console.log(`👤 Cliente conectado: ${socket.id}`);

    // Enviar estado actual de todos los símbolos al cliente nuevo
    SYMBOLS.forEach(meta => {
        const sym = meta.binance;
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

// ── REST endpoint opcional para debug ────────────────────────
app.get('/api/status', (req, res) => {
    res.json({
        symbols: SYMBOLS.map(s => ({
            name:    s.name,
            candles: candles[s.binance]?.length || 0,
            price:   prices[s.binance] || null
        }))
    });
});

// ── Startup ───────────────────────────────────────────────────
async function start() {
    console.log('🚀 Cargando historial de velas...');

    // Cargar historial secuencialmente para no saturar la API
    for (const s of SYMBOLS) {
        await loadHistory(s.binance);
        await new Promise(r => setTimeout(r, 300));
    }

    // Abrir WebSocket después de tener el historial
    openBinanceStream();

    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => {
        console.log(`✅ Servidor corriendo en puerto ${PORT}`);
    });
}

start();
