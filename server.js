const WebSocket = require('ws');
const express = require('express');
const http = require('http');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
const server = http.createServer(app);

const API_KEY = '88d88bb32b009088f09a386f5d9038473f59f36d';
const AIS_WS_URL = 'wss://stream.aisstream.io/v0/stream';

// Cache para navios já encontrados (evita buscar repetido)
const shipCache = new Map();
const CACHE_DURATION = 5 * 60 * 1000; // 5 minutos

// Servir frontend estático
app.use(express.static('public'));

// Rota de health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// API REST para buscar navio
app.get('/api/ship/:name', async (req, res) => {
    const shipName = req.params.name.toUpperCase().trim();
    console.log(`[${new Date().toLocaleTimeString()}] Buscando navio: ${shipName}`);
    
    // Verificar cache
    const cached = shipCache.get(shipName);
    if (cached && (Date.now() - cached.timestamp < CACHE_DURATION)) {
        console.log(`Cache hit para ${shipName}`);
        return res.json(cached.data);
    }
    
    try {
        const shipData = await searchShip(shipName);
        
        // Salvar no cache
        shipCache.set(shipName, {
            data: shipData,
            timestamp: Date.now()
        });
        
        res.json(shipData);
    } catch (error) {
        console.error(`Erro ao buscar ${shipName}:`, error.message);
        if (error.message.includes('Timeout')) {
            res.status(404).json({ 
                error: 'Navio não encontrado', 
                message: 'Timeout - navio não está transmitindo sinal AIS no momento'
            });
        } else {
            res.status(500).json({ 
                error: 'Erro interno', 
                message: error.message 
            });
        }
    }
});

function searchShip(shipName) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(AIS_WS_URL);
        let timeout;
        let found = false;
        let messageCount = 0;
        
        ws.on('open', () => {
            console.log(`WebSocket aberto para busca: ${shipName}`);
            
            const subscription = {
                APIKey: API_KEY,
                BoundingBoxes: [[[-90, -180], [90, 180]]],
                FilterMessageTypes: ["ShipStaticData", "PositionReport"]
            };
            
            ws.send(JSON.stringify(subscription));
            
            // Timeout de 20 segundos
            timeout = setTimeout(() => {
                if (!found) {
                    console.log(`Timeout para ${shipName} (${messageCount} mensagens processadas)`);
                    ws.close();
                    reject(new Error('Timeout'));
                }
            }, 20000);
        });
        
        ws.on('message', (data) => {
            try {
                messageCount++;
                const msg = JSON.parse(data);
                
                if (msg.MessageType === 'ShipStaticData' && msg.Message && msg.Message.ShipStaticData) {
                    const ship = msg.Message.ShipStaticData;
                    const name = ship.Name ? ship.Name.trim().toUpperCase() : '';
                    
                    // Log a cada 50 mensagens para debug
                    if (messageCount % 50 === 0) {
                        console.log(`Processadas ${messageCount} mensagens...`);
                    }
                    
                    // Match parcial do nome
                    if (name && (name.includes(shipName) || shipName.includes(name))) {
                        found = true;
                        clearTimeout(timeout);
                        ws.close();
                        
                        console.log(`✅ Navio encontrado: ${ship.Name} (IMO: ${ship.ImoNumber})`);
                        
                        resolve({
                            name: ship.Name,
                            imo: ship.ImoNumber,
                            callsign: ship.CallSign,
                            destination: ship.Destination ? ship.Destination.trim() : 'N/A',
                            eta: ship.Eta,
                            position: msg.MetaData ? {
                                lat: msg.MetaData.latitude,
                                lon: msg.MetaData.longitude
                            } : null,
                            timestamp: msg.MetaData ? msg.MetaData.time_utc : null,
                            type: ship.Type,
                            shipType: getShipType(ship.Type)
                        });
                    }
                }
            } catch (e) {
                console.error('Erro ao processar mensagem:', e.message);
            }
        });
        
        ws.on('error', (err) => {
            clearTimeout(timeout);
            console.error('WebSocket error:', err.message);
            reject(err);
        });
        
        ws.on('close', () => {
            if (!found) {
                clearTimeout(timeout);
            }
        });
    });
}

function getShipType(type) {
    const types = {
        30: 'Pesca', 60: 'Passageiros', 70: 'Carga', 71: 'Carga Perigosa',
        80: 'Tanque', 81: 'Tanque Perigoso'
    };
    return types[type] || 'Desconhecido';
}

// Limpar cache antigo a cada 10 minutos
setInterval(() => {
    const now = Date.now();
    let cleared = 0;
    for (let [key, value] of shipCache) {
        if (now - value.timestamp > CACHE_DURATION) {
            shipCache.delete(key);
            cleared++;
        }
    }
    if (cleared > 0) {
        console.log(`Cache limpo: ${cleared} entradas removidas`);
    }
}, 10 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Servidor rodando na porta ${PORT}`);
    console.log(`📡 Conectado à AISStream.io`);
    console.log(`⏱️ Timeout de busca: 20 segundos`);
    console.log(`💾 Cache: 5 minutos`);
});
