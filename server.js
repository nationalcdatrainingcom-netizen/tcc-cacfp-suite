require('dotenv').config();
const express = require('express');
const session = require('express-session');
const pgSession = require('connect-pg-simple')(session);
const path = require('path');
const { pool } = require('./db');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.set('trust proxy', 1);

// Session
app.use(session({
  store: new pgSession({ pool, createTableIfMissing: true }),
  secret: process.env.SESSION_SECRET || 'ff-faithful-foundations-secret',
  resave: false,
  saveUninitialized: false,
  cookie: { secure: 'auto', maxAge: 7 * 24 * 60 * 60 * 1000 }
}));

// Static files
app.use(express.static(path.join(__dirname, 'public')));

// Routes
app.use('/api/auth', require('./routes/auth'));
app.use('/api/explorations', require('./routes/explorations'));
app.use('/api/lessons', require('./routes/lessons'));
app.use('/api/admin', require('./routes/admin'));
app.use('/api/generate', require('./routes/generate'));
app.use('/api/pipeline', require('./routes/pipeline'));
app.use('/lesson', require('./routes/render'));

app.use('/api/vocab-image', require('./routes/vocabimage'));
app.get('/api/health', (req, res) => res.json({ status: 'ok', app: 'Faithful Foundations' }));

// SPA fallback
app.get('/*path', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// Init DB
async function initDB() {
  const fs = require('fs');
  const schema = fs.readFileSync(path.join(__dirname, 'db', 'schema.sql'), 'utf8');
  try {
    await pool.query(schema);
    console.log('Database initialized');
  } catch (err) {
    console.error('DB init error:', err.message);
  }
}

initDB().then(() => {
  app.listen(PORT, () => console.log(`Faithful Foundations running on port ${PORT}`));
});
