// db.js
// ---------------------- SQL SERVER ----------------------
const sql = require('mssql');

const sqlServerConfig = {
  user: process.env.SQL_USER,
  password: process.env.SQL_PASSWORD,
  server: process.env.SQL_SERVER_IP,
  database: process.env.SQL_DATABASE,
  options: {
    encrypt: false, // true se Azure
    trustServerCertificate: true
  }
};

async function conectarSQLServer() {
  try {
    const pool = await sql.connect(sqlServerConfig);
    console.log('Conectado ao SQL Server com sucesso!');
    return pool;
  } catch (err) {
    console.error('Erro ao conectar ao SQL Server:', err);
    throw err;
  }
}

// ---------------------- POSTGRESQL ----------------------
const { Pool } = require('pg');

const pgConfig = {
  user: process.env.PG_USER,
  host: process.env.PG_SERVER,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT, // padrão PostgreSQL
};

const pgPool = new Pool(pgConfig);

async function conectarPostgres() {
  try {
    const client = await pgPool.connect();
    console.log('Conectado ao PostgreSQL com sucesso!');
    return client;
  } catch (err) {
    console.error('Erro ao conectar ao PostgreSQL:', err);
    throw err;
  }
}

// ---------------------- POSTGRESQL2 ----------------------
const pgConfig2 = {
  user: process.env.PG2_USER,
  host: process.env.PG2_SERVER,
  database: process.env.PG2_DATABASE,
  password: process.env.PG2_PASSWORD,
  port: process.env.PG2_PORT, // padrão PostgreSQL
};

const pgPool2 = new Pool(pgConfig2);

async function conectarPostgres2() {
  try {
    const client = await pgPool2.connect();
    // console.log('Conectado ao PostgreSQL com sucesso!');
    return client;
  } catch (err) {
    console.error('Erro ao conectar ao PostgreSQL:', err);
    throw err;
  }
}

module.exports = {
  conectarSQLServer,
  sql,
  conectarPostgres,
  pgPool,
  conectarPostgres2,
  pgPool2
};