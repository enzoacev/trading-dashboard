const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const WebSocket  = require('ws');
const path       = require('path');
const crypto     = require('crypto');
const fs         = require('fs');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const KRAKEN_REST_URL = 'https://api.kraken.com/0/public';
const KRAKEN_PRIV_URL = 'https://api.kraken.com';
const KRAKEN_WS_URL   = 'wss://ws.kraken.com';
const CONFIG_FILE     = path.join(__dirname, '.auto-config.json');
const POSITIONS_FILE  = path.join(__dirname, '.positions.json');

// ── Config persistence ────────────────────────────────────────
function loadJson(file, def = {}) {
    try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return def; }
}
function saveJson(file, data) {
    fs.writeFileSync(file, JSON.stringify(data, null, 2));
}

const saved = loadJson(CONFIG_FILE);
let krakenApiKey    = process.env.KRAKEN_API_KEY    || saved.apiKey    || '';
let krakenApiSecret = process.env.KRAKEN_API_SECRET || saved.apiSecret || '';

const botConfig = {
    enabled:       saved.enabled       ?? false,
    stopLoss:      saved.stopLoss      ?? 3,
    takeProfit:    saved.takeProfit    ?? 5,
    tradeAmount:   saved.tradeAmount   ?? 50,
    minConfidence: saved.minConfidence ?? 70,
    cooldownMin:   saved.cooldownMin   ?? 30,
};

const botPositions   = loadJson(POSITIONS_FILE, {});
const lastSignals    = {};
const lastTradeTimes = {};
const botLog         = [];
let   isTrading      = false;

function addBotLog(msg, type = 'info') {
    const entry = { msg, type, time: new Date().toLocaleString('es-AR') };
    botLog.unshift(entry);
    if (botLog.length > 100) botLog.pop();
    io.emit('bot-log', entry);
    console.log(`🤖 [${type.toUpperCase()}] ${msg}`);
}

// ── Symbols ───────────────────────────────────────────────────
const SYMBOLS = [
    { id:'BTCUSDT', restPair:'XBTUSD', wsPair:'XBT/USD', name:'Bitcoin',  symbol:'BTC', tradePair:'XBTUSD', asset:'XXBT', minVol:0.0001, volDec:4 },
    { id:'ETHUSDT', restPair:'ETHUSD', wsPair:'ETH/USD', name:'Ethereum', symbol:'ETH', tradePair:'ETHUSD', asset:'XETH', minVol:0.01,   volDec:3 },
    { id:'ADAUSDT', restPair:'ADAUSD', wsPair:'ADA/USD', name:'Cardano',  symbol:'ADA', tradePair:'ADAUSD', asset:'ADA',  minVol:15,     volDec:0 },
    { id:'SOLUSDT', restPair:'SOLUSD', wsPair:'SOL/USD', name:'Solana',   symbol:'SOL', tradePair:'SOLUSD', asset:'SOL',  minVol:0.1,    volDec:2 },
    { id:'XRPUSDT', restPair:'XRPUSD', wsPair:'XRP/USD', name:'Ripple',   symbol:'XRP', tradePair:'XRPUSD', asset:'XXRP', minVol:15,     volDec:0 },
];

const wsPairToId = {};
SYMBOLS.forEach(s => { wsPairToId[s.wsPair] = s.id; });

const candles = {};
const prices  = {};
const signals = {};

// ── Technical Analysis ────────────────────────────────────────
const TA = {
    cls: cs => cs.map(c => c.c),
    sma(a,n){ if(a.length<n)return null; return a.slice(-n).reduce((x,y)=>x+y)/n },
    ema(a,n){
        if(a.length<n)return null;
        const k=2/(n+1); let e=a[0];
        for(let i=1;i<a.length;i++) e=a[i]*k+e*(1-k);
        return e;
    },
    macd(cls){
        if(cls.length<35)return null;
        const line=[];
        for(let e=cls.length-8;e<=cls.length;e++){
            const sl=cls.slice(0,e);
            const f=this.ema(sl,12),s=this.ema(sl,26);
            if(f!=null&&s!=null)line.push(f-s);
        }
        if(line.length<9)return null;
        const m=line[line.length-1],sig=this.ema(line,9);
        return{macd:m,signal:sig,histogram:sig!=null?m-sig:null};
    },
    atr(cs,n=14){
        if(cs.length<n+1)return null;
        let s=0;
        for(let i=cs.length-n;i<cs.length;i++){
            const h=cs[i].h,l=cs[i].l,pc=cs[i-1].c;
            s+=Math.max(h-l,Math.abs(h-pc),Math.abs(l-pc));
        }
        return s/n;
    },
    adx(cs,n=14){
        if(cs.length<n+1)return null;
        let p=0,nd=0,tr=0;
        for(let i=cs.length-n;i<cs.length;i++){
            const h=cs[i].h,l=cs[i].l,ph=cs[i-1].h,pl=cs[i-1].l,pc=cs[i-1].c;
            const u=h-ph,d=pl-l;
            if(u>d&&u>0)p+=u; if(d>u&&d>0)nd+=d;
            tr+=Math.max(h-l,Math.abs(h-pc),Math.abs(l-pc));
        }
        if(!tr)return null;
        const pdi=(p/tr)*100,ndi=(nd/tr)*100;
        return{adx:Math.abs(pdi-ndi)/(pdi+ndi||1)*100,pdi,ndi};
    },
    rsi(cls,n=14){
        if(cls.length<n+1)return null;
        let g=0,l=0;
        for(let i=cls.length-n;i<cls.length;i++){
            const d=cls[i]-cls[i-1]; if(d>0)g+=d; else l-=d;
        }
        return 100-100/(1+(g/n)/((l/n)||.0001));
    },
    stoch(cs,n=14){
        if(cs.length<n)return null;
        const sl=cs.slice(-n);
        const hi=Math.max(...sl.map(c=>c.h)),lo=Math.min(...sl.map(c=>c.l));
        return hi===lo?50:((cs[cs.length-1].c-lo)/(hi-lo))*100;
    },
    cci(cs,n=20){
        if(cs.length<n)return null;
        const tps=cs.slice(-n).map(c=>(c.h+c.l+c.c)/3);
        const mean=tps.reduce((a,b)=>a+b)/n;
        const md=tps.reduce((a,b)=>a+Math.abs(b-mean),0)/n;
        const ltp=(cs[cs.length-1].h+cs[cs.length-1].l+cs[cs.length-1].c)/3;
        return(ltp-mean)/(0.015*(md||.0001));
    },
    wr(cs,n=14){
        if(cs.length<n)return null;
        const sl=cs.slice(-n);
        const hi=Math.max(...sl.map(c=>c.h)),lo=Math.min(...sl.map(c=>c.l));
        return hi===lo?-50:((hi-cs[cs.length-1].c)/(hi-lo))*-100;
    },
    bb(cls,n=20){
        if(cls.length<n)return null;
        const sl=cls.slice(-n),sma=sl.reduce((a,b)=>a+b)/n;
        const std=Math.sqrt(sl.reduce((s,x)=>s+(x-sma)**2,0)/n);
        return{upper:sma+2*std,middle:sma,lower:sma-2*std};
    },
    kc(cs,n=20,m=1.5){
        const cls=this.cls(cs),b=this.ema(cls,n),r=this.atr(cs,n);
        if(!b||!r)return null;
        return{upper:b+m*r,lower:b-m*r};
    },
    dc(cs,n=20){
        if(cs.length<n)return null;
        const sl=cs.slice(-n);
        return{upper:Math.max(...sl.map(c=>c.h)),lower:Math.min(...sl.map(c=>c.l))};
    },
    obv(cs){
        if(cs.length<2)return null;
        let o=0;
        for(let i=1;i<cs.length;i++){
            if(cs[i].c>cs[i-1].c)o+=cs[i].v; else if(cs[i].c<cs[i-1].c)o-=cs[i].v;
        }
        return o;
    },
    vwap(cs,n=20){
        if(cs.length<n)return null;
        const sl=cs.slice(-n);
        const tpv=sl.reduce((a,c)=>a+(c.h+c.l+c.c)/3*c.v,0);
        const vol=sl.reduce((a,c)=>a+c.v,0);
        return vol>0?tpv/vol:null;
    },
    cmf(cs,n=20){
        if(cs.length<n)return null;
        let mfv=0,vol=0;
        for(const c of cs.slice(-n)){const r=c.h-c.l;if(r>0)mfv+=((c.c-c.l)-(c.h-c.c))/r*c.v;vol+=c.v}
        return vol>0?mfv/vol:0;
    },
    mfi(cs,n=14){
        if(cs.length<n+1)return null;
        let pos=0,neg=0;
        for(let i=cs.length-n;i<cs.length;i++){
            const tp=(cs[i].h+cs[i].l+cs[i].c)/3,ptp=(cs[i-1].h+cs[i-1].l+cs[i-1].c)/3;
            const fl=tp*cs[i].v; if(tp>ptp)pos+=fl; else if(tp<ptp)neg+=fl;
        }
        return neg===0?100:100-100/(1+pos/neg);
    },
    va(cs,n=20){
        if(cs.length<n+1)return{ratio:1,anomaly:false};
        const vols=cs.slice(-n-1,-1).map(c=>c.v);
        const avg=vols.reduce((a,b)=>a+b)/vols.length;
        const ratio=avg>0?cs[cs.length-1].v/avg:1;
        return{ratio,anomaly:ratio>2};
    },
    psar(cls,step=0.02,max=0.2){
        if(cls.length<4)return null;
        let bull=true,sar=cls[0],ep=cls[1],af=step;
        for(let i=2;i<cls.length;i++){
            const p=sar; sar=p+af*(ep-p);
            if(bull){if(cls[i]>ep){ep=cls[i];af=Math.min(af+step,max)}if(cls[i]<sar){bull=false;sar=ep;ep=cls[i];af=step}}
            else{if(cls[i]<ep){ep=cls[i];af=Math.min(af+step,max)}if(cls[i]>sar){bull=true;sar=ep;ep=cls[i];af=step}}
        }
        return{sar,bull};
    },
    ichi(cs){
        if(cs.length<52)return null;
        const h=n=>Math.max(...cs.slice(-n).map(c=>c.h));
        const l=n=>Math.min(...cs.slice(-n).map(c=>c.l));
        const t=(h(9)+l(9))/2,k=(h(26)+l(26))/2;
        return{senkouA:(t+k)/2,senkouB:(h(52)+l(52))/2};
    },
    mom(cls,n=10){if(cls.length<n+1)return null;return cls[cls.length-1]-cls[cls.length-1-n]},
    roc(cls,n=12){
        if(cls.length<n+1)return null;
        const p=cls[cls.length-1-n];
        return((cls[cls.length-1]-p)/p)*100;
    },
};

function getRec(cs,price){
    if(!cs||cs.length<30)return{rec:'ESPERAR',confidence:0,signals:[],ind:{}};
    const cls=TA.cls(cs); let buy=0,sell=0; const sigs=[];

    const s50=TA.sma(cls,50),s200=TA.sma(cls,200);
    if(s50&&s200){if(s50>s200){buy+=8;sigs.push({t:'bull',s:'Golden Cross SMA50>200'})}else{sell+=8;sigs.push({t:'bear',s:'Death Cross SMA50<200'})}}
    const e21=TA.ema(cls,21);
    if(e21){if(price>e21){buy+=5;sigs.push({t:'bull',s:'Precio sobre EMA21'})}else{sell+=5;sigs.push({t:'bear',s:'Precio bajo EMA21'})}}
    const macd=TA.macd(cls);
    if(macd){if(macd.macd>macd.signal){buy+=8;sigs.push({t:'bull',s:`MACD alcista (hist:${macd.histogram?.toFixed(2)})`})}else{sell+=8;sigs.push({t:'bear',s:`MACD bajista (hist:${macd.histogram?.toFixed(2)})`})}}
    const adx=TA.adx(cs);
    if(adx){if(adx.adx>25){if(adx.pdi>adx.ndi){buy+=7;sigs.push({t:'bull',s:`ADX ${adx.adx.toFixed(0)}: tendencia alcista`})}else{sell+=7;sigs.push({t:'bear',s:`ADX ${adx.adx.toFixed(0)}: tendencia bajista`})}}else sigs.push({t:'neut',s:`ADX ${adx.adx.toFixed(0)}: mercado lateral`})}
    const ps=TA.psar(cls);
    if(ps){if(ps.bull){buy+=5;sigs.push({t:'bull',s:'Parabolic SAR alcista'})}else{sell+=5;sigs.push({t:'bear',s:'Parabolic SAR bajista'})}}
    const ich=TA.ichi(cs);
    if(ich){if(price>ich.senkouA&&price>ich.senkouB){buy+=5;sigs.push({t:'bull',s:'Ichimoku: sobre nube'})}else if(price<ich.senkouA&&price<ich.senkouB){sell+=5;sigs.push({t:'bear',s:'Ichimoku: bajo nube'})}}
    const rsi=TA.rsi(cls);
    if(rsi!=null){if(rsi<30){buy+=8;sigs.push({t:'bull',s:`RSI ${rsi.toFixed(1)}: sobreventa`})}else if(rsi>70){sell+=8;sigs.push({t:'bear',s:`RSI ${rsi.toFixed(1)}: sobrecompra`})}else sigs.push({t:'neut',s:`RSI ${rsi.toFixed(1)}: neutro`})}
    const stoch=TA.stoch(cs);
    if(stoch!=null){if(stoch<20){buy+=6;sigs.push({t:'bull',s:`Estoc. ${stoch.toFixed(1)}: compra`})}else if(stoch>80){sell+=6;sigs.push({t:'bear',s:`Estoc. ${stoch.toFixed(1)}: venta`})}}
    const cci=TA.cci(cs);
    if(cci!=null){if(cci<-100){buy+=5;sigs.push({t:'bull',s:`CCI ${cci.toFixed(0)}: sobreventa`})}else if(cci>100){sell+=5;sigs.push({t:'bear',s:`CCI ${cci.toFixed(0)}: sobrecompra`})}}
    const wr=TA.wr(cs);
    if(wr!=null){if(wr<-80){buy+=5;sigs.push({t:'bull',s:`W%R ${wr.toFixed(0)}: compra`})}else if(wr>-20){sell+=5;sigs.push({t:'bear',s:`W%R ${wr.toFixed(0)}: venta`})}}
    const mom=TA.mom(cls);
    if(mom!=null){if(mom>0){buy+=3;sigs.push({t:'bull',s:`Momentum +${mom.toFixed(2)}`})}else{sell+=3;sigs.push({t:'bear',s:`Momentum ${mom.toFixed(2)}`})}}
    const roc=TA.roc(cls);
    if(roc!=null){if(roc>5){buy+=3;sigs.push({t:'bull',s:`ROC +${roc.toFixed(1)}%`})}else if(roc<-5){sell+=3;sigs.push({t:'bear',s:`ROC ${roc.toFixed(1)}%`})}}
    const bb=TA.bb(cls);
    if(bb){if(price<bb.lower){buy+=6;sigs.push({t:'bull',s:'Bajo banda BB → rebote'})}else if(price>bb.upper){sell+=6;sigs.push({t:'bear',s:'Sobre banda BB → corrección'})}else sigs.push({t:'neut',s:'Dentro de Bandas Bollinger'})}
    const atr=TA.atr(cs);
    if(atr){sigs.push({t:'neut',s:`ATR: ${((atr/price)*100).toFixed(2)}% volatilidad`})}
    const kc=TA.kc(cs);
    if(kc){if(price<kc.lower){buy+=4;sigs.push({t:'bull',s:'Bajo canal Keltner'})}else if(price>kc.upper){sell+=4;sigs.push({t:'bear',s:'Sobre canal Keltner'})}}
    const dc=TA.dc(cs);
    if(dc){if(price>=dc.upper*.99){sell+=3;sigs.push({t:'bear',s:'Resistencia Donchian 20D'})}else if(price<=dc.lower*1.01){buy+=3;sigs.push({t:'bull',s:'Soporte Donchian 20D'})}}
    const obv=TA.obv(cs);
    if(obv!=null){if(obv>0){buy+=3;sigs.push({t:'bull',s:'OBV: flujo comprador'})}else{sell+=3;sigs.push({t:'bear',s:'OBV: flujo vendedor'})}}
    const vwap=TA.vwap(cs);
    if(vwap){if(price>vwap){buy+=3;sigs.push({t:'bull',s:`Sobre VWAP ($${vwap.toFixed(2)})`})}else{sell+=3;sigs.push({t:'bear',s:`Bajo VWAP ($${vwap.toFixed(2)})`})}}
    const cmf=TA.cmf(cs);
    if(cmf!=null){if(cmf>0.1){buy+=3;sigs.push({t:'bull',s:`CMF ${cmf.toFixed(2)}: positivo`})}else if(cmf<-0.1){sell+=3;sigs.push({t:'bear',s:`CMF ${cmf.toFixed(2)}: negativo`})}}
    const mfi=TA.mfi(cs);
    if(mfi!=null){if(mfi<20){buy+=3;sigs.push({t:'bull',s:`MFI ${mfi.toFixed(0)}: sobreventa`})}else if(mfi>80){sell+=3;sigs.push({t:'bear',s:`MFI ${mfi.toFixed(0)}: sobrecompra`})}}
    const va=TA.va(cs);
    if(va.anomaly){buy+=3;sigs.push({t:'bull',s:`Vol. anomalía: ${va.ratio.toFixed(1)}x`})}
    if(cs.length>=24){const p24=cs[cs.length-24].c,ch=((price-p24)/p24)*100;if(ch>5){sell+=2;sigs.push({t:'bear',s:`Suba 24h +${ch.toFixed(1)}%`})}else if(ch<-5){buy+=2;sigs.push({t:'bull',s:`Caída 24h ${ch.toFixed(1)}%`})}}

    const total=buy+sell;
    const conf=total>0?Math.round((Math.max(buy,sell)/total)*100):50;
    let rec='ESPERAR';
    if(buy>sell+15)rec='COMPRA';
    else if(sell>buy+15)rec='VENTA';
    return{rec,confidence:conf,signals:sigs.slice(0,10),ind:{rsi,stoch,macd,adx:adx?.adx,atr,mfi,cci,wr,obv:obv!=null?(obv>0?'↑':'↓'):'—',vwap,cmf}};
}

// ── Kraken Private API ─────────────────────────────────────────
async function krakenPrivate(endpoint, params = {}) {
    if (!krakenApiKey || !krakenApiSecret) throw new Error('API keys no configuradas');
    const nonce    = Date.now().toString();
    const postData = new URLSearchParams({ nonce, ...params }).toString();
    const sha256   = crypto.createHash('sha256').update(nonce + postData).digest();
    const key      = Buffer.from(krakenApiSecret, 'base64');
    const hmac     = crypto.createHmac('sha512', key);
    hmac.update(endpoint);
    hmac.update(sha256);
    const sign = hmac.digest('base64');

    const res = await fetch(`${KRAKEN_PRIV_URL}${endpoint}`, {
        method: 'POST',
        headers: {
            'API-Key':      krakenApiKey,
            'API-Sign':     sign,
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: postData,
    });
    const json = await res.json();
    if (json.error?.length) throw new Error(json.error[0]);
    return json.result;
}

async function getBalance() {
    const r = await krakenPrivate('/0/private/Balance');
    return {
        USD: parseFloat(r.ZUSD || r.USD || 0),
        BTC: parseFloat(r.XXBT || r.XBT || 0),
        ETH: parseFloat(r.XETH || r.ETH || 0),
        ADA: parseFloat(r.ADA  || 0),
        SOL: parseFloat(r.SOL  || 0),
        XRP: parseFloat(r.XXRP || r.XRP || 0),
    };
}

// ── Auto-trading engine ────────────────────────────────────────
async function openPosition(sym, price) {
    isTrading = true;
    try {
        const meta    = SYMBOLS.find(s => s.id === sym);
        const balance = await getBalance();
        if (balance.USD < botConfig.tradeAmount) {
            addBotLog(`⚠️ Balance USD insuficiente: $${balance.USD.toFixed(2)} (necesita $${botConfig.tradeAmount})`, 'error');
            return;
        }
        const rawVol = botConfig.tradeAmount / price;
        const volume = parseFloat(Math.max(rawVol, meta.minVol).toFixed(meta.volDec));

        addBotLog(`🟢 Abriendo COMPRA ${meta.symbol} — $${price.toFixed(2)} — ${volume} ${meta.symbol}`, 'buy');

        const order = await krakenPrivate('/0/private/AddOrder', {
            pair:      meta.tradePair,
            type:      'buy',
            ordertype: 'market',
            volume:    volume.toFixed(meta.volDec),
        });

        botPositions[sym] = {
            entryPrice: price,
            volume,
            orderId: order.txid?.[0] || null,
            time:    new Date().toLocaleString('es-AR'),
            sym,
        };
        saveJson(POSITIONS_FILE, botPositions);
        lastTradeTimes[sym] = Date.now();

        addBotLog(`✅ COMPRA ejecutada: ${volume} ${meta.symbol} @ ~$${price.toFixed(2)}`, 'buy');
        io.emit('bot-update', { positions: botPositions, log: botLog.slice(0, 30) });
    } catch (e) {
        addBotLog(`❌ Error abriendo posición ${sym}: ${e.message}`, 'error');
    } finally {
        isTrading = false;
    }
}

async function closePosition(sym, price, reason) {
    const pos  = botPositions[sym];
    if (!pos) return;
    isTrading = true;
    try {
        const meta = SYMBOLS.find(s => s.id === sym);
        addBotLog(`🔴 Cerrando ${meta.symbol} — razón: ${reason}`, 'sell');

        await krakenPrivate('/0/private/AddOrder', {
            pair:      meta.tradePair,
            type:      'sell',
            ordertype: 'market',
            volume:    pos.volume.toFixed(meta.volDec),
        });

        const pct = ((price - pos.entryPrice) / pos.entryPrice) * 100;
        delete botPositions[sym];
        saveJson(POSITIONS_FILE, botPositions);
        lastTradeTimes[sym] = Date.now();

        addBotLog(`✅ VENTA ejecutada: ${meta.symbol} @ ~$${price.toFixed(2)} | P&L: ${pct>=0?'+':''}${pct.toFixed(2)}%`, 'sell');
        io.emit('bot-update', { positions: botPositions, log: botLog.slice(0, 30) });
    } catch (e) {
        addBotLog(`❌ Error cerrando posición ${sym}: ${e.message}`, 'error');
    } finally {
        isTrading = false;
    }
}

async function checkAutoTrade(sym, rec, price, confidence) {
    if (!botConfig.enabled || !krakenApiKey || isTrading) return;

    const now      = Date.now();
    const prevRec  = lastSignals[sym];
    const hasPos   = !!botPositions[sym];

    // Always update last signal
    lastSignals[sym] = rec;

    // Check stop-loss / take-profit on open positions first
    if (hasPos) {
        const pos = botPositions[sym];
        const pct = ((price - pos.entryPrice) / pos.entryPrice) * 100;
        if (pct <= -botConfig.stopLoss) {
            await closePosition(sym, price, `STOP-LOSS (${pct.toFixed(2)}%)`);
            return;
        }
        if (pct >= botConfig.takeProfit) {
            await closePosition(sym, price, `TAKE-PROFIT (+${pct.toFixed(2)}%)`);
            return;
        }
    }

    // Only act on signal changes
    if (prevRec === rec) return;
    if (confidence < botConfig.minConfidence) return;

    if (rec === 'COMPRA' && !hasPos) {
        const cooldownMs = botConfig.cooldownMin * 60 * 1000;
        if ((now - (lastTradeTimes[sym] || 0)) < cooldownMs) return;
        await openPosition(sym, price);
    }

    if ((rec === 'VENTA' || rec === 'ESPERAR') && hasPos) {
        await closePosition(sym, price, `SEÑAL ${rec}`);
    }
}

// ── Bot REST API ──────────────────────────────────────────────
app.post('/api/bot/keys', (req, res) => {
    const { apiKey, apiSecret } = req.body;
    if (!apiKey || !apiSecret) return res.status(400).json({ error: 'Keys requeridas' });
    krakenApiKey    = apiKey.trim();
    krakenApiSecret = apiSecret.trim();
    const prev = loadJson(CONFIG_FILE);
    saveJson(CONFIG_FILE, { ...prev, apiKey: krakenApiKey, apiSecret: krakenApiSecret });
    addBotLog('🔑 API keys configuradas', 'info');
    res.json({ ok: true });
});

app.delete('/api/bot/keys', (req, res) => {
    krakenApiKey = krakenApiSecret = '';
    const prev = loadJson(CONFIG_FILE);
    delete prev.apiKey; delete prev.apiSecret;
    saveJson(CONFIG_FILE, prev);
    res.json({ ok: true });
});

app.get('/api/bot/status', async (req, res) => {
    const hasKeys = !!(krakenApiKey && krakenApiSecret);
    let balance = null;
    if (hasKeys) {
        try { balance = await getBalance(); } catch(e) { balance = { error: e.message }; }
    }
    res.json({
        enabled:   botConfig.enabled,
        hasKeys,
        balance,
        config:    { ...botConfig },
        positions: botPositions,
        log:       botLog.slice(0, 30),
        signals:   Object.fromEntries(
            Object.entries(signals).map(([k,v]) => [k, { rec: v.rec, confidence: v.confidence }])
        ),
    });
});

app.post('/api/bot/toggle', (req, res) => {
    botConfig.enabled = !botConfig.enabled;
    const prev = loadJson(CONFIG_FILE);
    saveJson(CONFIG_FILE, { ...prev, enabled: botConfig.enabled });
    addBotLog(`Bot ${botConfig.enabled ? 'ACTIVADO' : 'DESACTIVADO'}`, 'info');
    io.emit('bot-config', { ...botConfig });
    res.json({ enabled: botConfig.enabled });
});

app.post('/api/bot/config', (req, res) => {
    const fields = ['stopLoss','takeProfit','tradeAmount','minConfidence','cooldownMin'];
    fields.forEach(f => { if (req.body[f] !== undefined) botConfig[f] = parseFloat(req.body[f]); });
    const prev = loadJson(CONFIG_FILE);
    saveJson(CONFIG_FILE, { ...prev, ...botConfig });
    io.emit('bot-config', { ...botConfig });
    res.json(botConfig);
});

app.post('/api/bot/close/:sym', async (req, res) => {
    const sym = req.params.sym;
    if (!botPositions[sym]) return res.status(404).json({ error: 'Sin posición abierta' });
    const price = prices[sym];
    if (!price) return res.status(400).json({ error: 'Precio no disponible' });
    try {
        await closePosition(sym, price, 'CIERRE MANUAL');
        res.json({ ok: true });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// ── Public REST ────────────────────────────────────────────────
app.get('/api/status', (req, res) => {
    res.json({
        source: 'Kraken',
        uptime: process.uptime(),
        botEnabled: botConfig.enabled,
        symbols: SYMBOLS.map(s => ({
            id: s.id, name: s.name,
            candles: candles[s.id]?.length || 0,
            price:   prices[s.id] || null,
            signal:  signals[s.id]?.rec || null,
        }))
    });
});

// ── Kraken history loader ─────────────────────────────────────
async function loadHistory(sym) {
    try {
        const meta = SYMBOLS.find(s => s.id === sym);
        const url  = `${KRAKEN_REST_URL}/OHLC?pair=${meta.restPair}&interval=60`;
        console.log(`📡 Cargando: ${url}`);
        const res  = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        if (json.error?.length) throw new Error(json.error[0]);
        const key  = Object.keys(json.result).find(k => k !== 'last');
        const list = json.result[key];
        candles[sym] = list.slice(-500).map(k => ({
            t: parseInt(k[0])*1000, o: parseFloat(k[1]), h: parseFloat(k[2]),
            l: parseFloat(k[3]),   c: parseFloat(k[4]), v: parseFloat(k[6]),
        }));
        prices[sym] = candles[sym][candles[sym].length-1].c;
        console.log(`✅ ${sym}: ${candles[sym].length} velas, precio: $${prices[sym]}`);
    } catch(e) {
        console.error(`❌ Error cargando ${sym}:`, e.message);
    }
}

// ── Kraken WebSocket ──────────────────────────────────────────
let pingInterval = null;

function openStream() {
    const ws = new WebSocket(KRAKEN_WS_URL);

    ws.on('open', () => {
        console.log('🔌 WebSocket Kraken conectado');
        ws.send(JSON.stringify({
            event: 'subscribe',
            pair:  SYMBOLS.map(s => s.wsPair),
            subscription: { name: 'ohlc', interval: 60 }
        }));
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ event: 'ping' }));
        }, 20000);
    });

    ws.on('message', (raw) => {
        try {
            const msg = JSON.parse(raw.toString());
            if (msg.event) return;
            if (!Array.isArray(msg) || msg.length < 4) return;

            const sym = wsPairToId[msg[3]];
            if (!sym) return;

            const data   = msg[1];
            const candle = {
                t: parseFloat(data[0])*1000, o: parseFloat(data[2]),
                h: parseFloat(data[3]),       l: parseFloat(data[4]),
                c: parseFloat(data[5]),       v: parseFloat(data[7]),
            };
            if (!candle.c) return;

            prices[sym] = candle.c;
            if (!candles[sym]) candles[sym] = [];

            const last = candles[sym][candles[sym].length-1];
            if (last && last.t === candle.t) {
                candles[sym][candles[sym].length-1] = candle;
            } else {
                candles[sym].push(candle);
                if (candles[sym].length > 500) candles[sym].shift();
            }

            const result = getRec(candles[sym], candle.c);
            signals[sym] = result;

            // Run auto-trade check asynchronously (don't block WS)
            checkAutoTrade(sym, result.rec, candle.c, result.confidence).catch(e =>
                console.error('Auto-trade error:', e.message)
            );

            const meta = SYMBOLS.find(s => s.id === sym);
            io.emit('update', {
                symbol:      sym,
                name:        meta.name,
                sym:         meta.symbol,
                price:       candle.c,
                closed:      false,
                candles:     candles[sym].slice(-200),
                result,
                botPosition: botPositions[sym] || null,
            });
        } catch(e) {
            console.error('❌ WS parse error:', e.message);
        }
    });

    ws.on('close', (code) => {
        console.log(`⚠️ WS cerrado (${code}) — reconectando en 5s`);
        if (pingInterval) clearInterval(pingInterval);
        setTimeout(openStream, 5000);
    });

    ws.on('error', e => console.error('❌ WS error:', e.message));
}

// ── Socket.IO ─────────────────────────────────────────────────
io.on('connection', (socket) => {
    console.log(`👤 Cliente: ${socket.id}`);
    SYMBOLS.forEach(meta => {
        const sym = meta.id;
        if (!candles[sym]?.length || !prices[sym]) return;
        socket.emit('update', {
            symbol:      sym,
            name:        meta.name,
            sym:         meta.symbol,
            price:       prices[sym],
            closed:      false,
            candles:     candles[sym].slice(-200),
            result:      signals[sym] || null,
            botPosition: botPositions[sym] || null,
        });
    });
    // Send current bot state on connect
    socket.emit('bot-config', { ...botConfig });
    socket.on('disconnect', () => console.log(`👤 Desconectado: ${socket.id}`));
});

// ── Startup ───────────────────────────────────────────────────
async function start() {
    console.log('🚀 Iniciando servidor...');
    for (const s of SYMBOLS) {
        await loadHistory(s.id);
        await new Promise(r => setTimeout(r, 300));
    }
    openStream();
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => {
        console.log(`✅ Servidor en puerto ${PORT}`);
        if (botConfig.enabled) addBotLog('Bot retomado (estaba activo al cerrar)', 'info');
    });
}

start();
