const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const cors = require('cors');
const WebSocket = require('ws');
const schedule = require('node-schedule');
const axios = require("axios");
const { parse } = require('path');
const app = express();

if(process.env.NODE_ENV != 'production') {
    require('dotenv').config();
}

const port = process.env.PORT || 3000;

const allowedOrigins = [process.env.CORS_URL, 'http://localhost:3000'];

const URL = process.env.URL;
const TOKEN = process.env.TOKEN;


const QUOTE_URL = process.env.QUOTE_URL;


const httpServer = http.createServer(app, {
    cors: {
      origin: allowedOrigins,
      methods: ["GET", "POST"]
    }
});

const options = {
    TOKEN: TOKEN,
    symbols: [
        'AAPL', 'MSFT', 'GILD', 'XOM', 'AMAT', 'AMZN', 'AMD', 'BABA', 
        'BAC', 'C', 'CVX', 'DIS', 'EBAY', 'GLD', 'INTC', 'WFC', 'WDC', 
        'JD', 'JPM', 'KO', 'LVS', 'META', 'MS', 'MU', 'NKE', 'NUGT', 
        'ORCL', 'PYPL', 'QCOM', 'ROKU', 'SBUX', 'SQ', 'SPY', 'UAL', 
        'UBER', 'VZ'
    ],
    connectionInterval: 15000,
}

const forbiddenValues = 
[ 
    "3", "5", "9", "10", "11", "12", "15", "16", "17", "18", 
    "20", "21", "22", "24", "25", "26", "27", "30", "31", "32",
    "34", "36", "37", "38", "40", "41"
];

class Queue{
    #items = [];

    enqueue(item) {
        this.#items.push(item);
    }

    dequeue() {
        return this.#items.shift();
    }

    isEmpty() {
        return this.#items.length === 0;
    }
}

const bars = options.symbols.reduce((accumulator, item) => {
    accumulator[item] = { "c": [], "p": 172.25, "s": item, "t": 0, "v": 0 };
    if(item == 'KO') accumulator[item] = { "c": [], "p": 60.49, "s": item, "t": 0, "v": 0, "ch": 0, "pc": 0 };
    if(item == 'GILD') accumulator[item] = { "c": [], "p": 72.57, "s": item, "t": 0, "v": 0 };
    if(item == 'JD') accumulator[item] = { "c": [], "p": 26.49, "s": item, "t": 0, "v": 0 };
    return accumulator;
}, {});


const io = socketIO(httpServer);

app.use(cors());
app.use(express.json())

class Server {
    constructor(io, UserManagement,options){
        this.io = io
        this.symbols = options.symbols
        this.connectionInterval = options.connectionInterval
        this.token = options.TOKEN
        this.socketServer
        this.lastConnectionTime
        this.um = UserManagement
        this.SocketClient
    }

    init() {
        this.finhubConnection()
        this.socketClient()
        this.eventListeners()
    }

    finhubConnection() {
        this.socketServer = new WebSocket(`wss://ws.finnhub.io?token=${this.token}`);
    }

    eventListeners() {

        this.socketServer.addEventListener('open', () => {
            console.log('Connected to finhub sucessfully...');
            this.serverResubscribe()
            this.pingInterval = setInterval(() => {
                if (this.socketServer.readyState === this.socketServer.OPEN) {
                    this.socketServer.ping();
                }
            }, this.connectionInterval);
        });
    
        this.socketServer.addEventListener('message', (event) => {
            const eventData = JSON.parse(event.data);
            if (eventData.type === "trade") {
                const record = eventData.data.pop();
                bars[record.s].p = record.p;
                bars[record.s].t = record.t;
                bars[record.s].v = record.v;
                bars[record.s].ch = record.p - bars[record.s].pc;
                record.ch = bars[record.s].ch;
                this.io.to(record.s).emit('message', JSON.stringify(record));
                this.io.to('orderServer').emit('message', JSON.stringify(record));
            }
        });
    
        this.socketServer.addEventListener('close', () => {
            console.log('Connection closed, retrying...');
            clearInterval(this.pingInterval)
            setTimeout(() => this.init(), this.connectionInterval);
        });
    
        this.socketServer.addEventListener('error', (error) => {
            console.log("Error happened, retrying...");
        });
    }

    onConnection(socket) {
        this.um.addUser(socket.id)
        this.serverLog(socket.id, true)
        socket.emit("message", 'Conectado');
        
        socket.on("subscribe", (symbol) => {
            if (this.symbols.includes(symbol)){
                if (!this.um.isRoomExists(symbol)) {
                    this.um.addRoom(symbol)
                    this.um.addUserToRoom(symbol, socket.id)
                    socket.join(symbol);
                } else {
                    if (!this.um.isUserInsideRoom(socket.id, symbol)) {
                        this.um.addUserToRoom(symbol, socket.id)
                        socket.join(symbol);
                    }
                }
                socket.emit("message", JSON.stringify(bars[symbol]));
            } else if (symbol == 'orderServer') {
                if (!this.um.isRoomExists(symbol)) {
                    this.um.addRoom(symbol)
                    this.um.addUserToRoom(symbol, socket.id)
                    socket.join(symbol);
                } else {
                    if (!this.um.isUserInsideRoom(socket.id, symbol)) {
                        this.um.addUserToRoom(symbol, socket.id)
                        socket.join(symbol);
                    }
                }
            } else {
                socket.emit("message", 'Symbol not found');
                socket.disconnect()
            }
            
        });
    
        socket.on("unsubscribe", (symbol) => {
            if (this.um.isUserInsideRoom(socket.id, symbol)) {
                this.um.removeUserFromRoom(symbol, socket.id)
                this.um.purgeAllEmptyRooms(this.serverUnsubscribeAll)
                socket.leave(symbol);
            }
        });
        
        socket.on("upgrade", (symbol) => {
            socket.emit("message", JSON.stringify(bars[symbol]));
        });
    
        socket.on("disconnect", () => {
            this.um.removeUser(socket.id)
            this.um.removeUserWithoutSymbol(socket.id, this.serverUnsubscribe)
            this.serverLog(socket.id, false)
        })
        
    }
    
    serverSubscribe(symbol) {
        this.socketServer.send(JSON.stringify({ 'type': 'subscribe', 'symbol': symbol }));
    }

    serverUnsubscribe = (symbol) => {
        // this.socketServer.send(JSON.stringify({ 'type': 'unsubscribe', 'symbol': symbol }));
    }

    serverUnsubscribeAll = (symbols) => {
        symbols.forEach(symbol => {
            // this.socketServer.send(JSON.stringify({ 'type': 'unsubscribe', 'symbol': symbol }));
        })
    }

    serverResubscribe() {
        options.symbols.forEach(symbol => {
            setTimeout(() => this.serverSubscribe(symbol), 100);
        })
    }

    serverLog (id, connection) {
        const currentDate = new Date();
        const year = currentDate.getFullYear();
        const month = String(currentDate.getMonth() + 1).padStart(2, '0');
        const day = String(currentDate.getDate()).padStart(2, '0');
        const hours = String(currentDate.getHours()).padStart(2, '0');
        const minutes = String(currentDate.getMinutes()).padStart(2, '0');
        const seconds = String(currentDate.getSeconds()).padStart(2, '0');
        const formattedDate = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;


        if (connection) {
            console.log(`User ${id} connected -- Time: ${formattedDate} -- Online Users: ${this.um.getCurrentUsers()}`);
        } else {
            console.log(`User ${id} disconnected -- Time: ${formattedDate} -- Online Users: ${this.um.getCurrentUsers()}`);
        }
    }
}

class UserManagement {
    constructor () {
        this.serverRooms = new Map()
        this.currentUsers = new Set()
    }


    addRoom(symbol) {
        this.serverRooms.set(symbol, new Set());
    }

    removeRoom(symbol) {
        this.serverRooms.delete(symbol)
    }

    isRoomExists(symbol) {
        return this.serverRooms.has(symbol)
    }

    isUserInsideRoom(id, symbol) {
        for (const [key, set] of this.serverRooms.entries()) {
            if (key == symbol) {
              if (set.has(id)) {
                return true
              } else {
                return false
              }
            } else {
                return false
            }
        }
    }

    getAllEmptyRooms() {
        const rooms = [];

        for (const [key, set] of this.serverRooms.entries()) {
            if (set.size == 0) {
                rooms.push(key);
            }
        }

        return rooms;
    }

    getAllRooms() {
        const rooms = this.serverRooms.keys();
        const roomsArray = Array.from(rooms);
        return roomsArray
    }

    removeAllEmptyRooms(rooms) {
        rooms.forEach((room) => {
            this.removeRoom(room)
        })
    }

    purgeAllEmptyRooms(callback) {
        const emptyRooms = this.getAllEmptyRooms()
        callback(emptyRooms)
        this.removeAllEmptyRooms(emptyRooms)
    }

    addUser(id) {
        this.currentUsers.add(id)
    }

    removeUser(id) {
        this.currentUsers.delete(id)
    }

    addUserToRoom(symbol, id) {
        this.serverRooms.get(symbol).add(id);
    }

    removeUserFromRoom(symbol, id) {
        const userRoom = this.serverRooms.get(symbol)
        userRoom.delete(id)
    }
    
    removeUserWithoutSymbol(id, callback) {
        for (const [key, set] of this.serverRooms.entries()) {
            if (set.has(id)) {
              set.delete(id);
              this.currentUsers.delete(id)
              if (set.size <= 0) {
                callback(key)
                this.removeRoom(key)
              }
            }
        }
    }

    getCurrentUsers() {
        return this.currentUsers.size
    }

    umLog() {
        console.log("serverRooms:", this.serverRooms);
        console.log("CurrentUsers:", this.currentUsers);
    }
}




const Users = new UserManagement()
const Finhub = new Server(io, Users, options)
Finhub.init()

io.on("connection", (socket) => {
    Finhub.onConnection(socket)
})

app.post('/candles',async (req, res) => {
    try {
        const symbol = req.body.symbol
        const resolution = req.body.resolution
        const from = req.body.from
        const to = req.body.to
        
        const response = await axios.get(`${URL}/api/v1/stock/candle?symbol=${symbol}&resolution=${resolution}&from=${from}&to=${to}&token=${TOKEN}`); // Replace with the actual API endpoint
        const data = response.data;
        
        res.json(data);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch data from the API' });
    }
});

httpServer.listen(port, () => {
    console.log(`Server listening on port ${port}`);
});


/************************** queue management  ********************************/

const quoteQueue = new Queue();

function fetchWaiting(symbol, url) {
    return async () => {
        try {
            const response = await axios.get(`${QUOTE_URL}${url}?symbol=${symbol}&resolution=D&limit=1&token=${TOKEN}`); // Replace with the actual API endpoint
            response.data.symbol = symbol; // Agregamos la propiedad symbol a la respuesta
            return response.data;
        } catch (error) {
            return {error: `Failed to fetch data of ${symbol} from the API`};
        }
    }
}

function addQueue() {
    options.symbols.forEach(item => {
        quoteQueue.enqueue(fetchWaiting(item, '/stock/candle', ));
    })
}

async function run() {
    while(!quoteQueue.isEmpty()) {
        const fn = quoteQueue.dequeue();
        const data = await fn();
        if('error' in data) {
            console.log(data.error)
        } else {
            bars[data.symbol].pc = data.c[0];
            bars[data.symbol].ch = bars[data.symbol].p - data.c[0];
        }
    }
    addQueue();
    console.log(bars);
}
/************************** END queue management  ********************************/

/************************** Schedule management  ********************************/
const myScheduleFunction = () =>  {
    console.log("Schedule function executed at: ", new Date());
    run();
}

const utc_time = '0 11 * * 1-5';
const scheduleJob = schedule.scheduleJob(utc_time, myScheduleFunction);
/************************** END Schedule management  ********************************/