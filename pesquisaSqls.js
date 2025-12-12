require('dotenv').config();

const { conectarSQLServer, conectarPostgres, conectarPostgres2, sql, pgPool } = require('./conexao');

// SQL Server
async function buscarSQLServer() {
  let count = 0;
    let querySql = `
      SELECT COD_UNI, UNI_NOME
      FROM AA_Unidades_Empresas
      WHERE EXISTS (
        SELECT 1
          FROM master..spt_values v
        WHERE v.type = 'P'
          AND v.number BETWEEN 1 AND LEN(UNI_NOME)
          AND UNICODE(SUBSTRING(UNI_NOME, v.number, 1)) = 65533
      );
    `;

    const pool = await conectarSQLServer();
    const result = await pool.request().query(querySql);
      result.recordset.forEach((retorno) =>{
        if(retorno) count += 1;
      })
        console.log('Quantidade de retornos SqlServer: ', count);
    sql.close();
    return result.recordset;
}

async function atualizarUnidadeTeste(cod_uni, uni_nome) {
  let count = 0;

  const querySql = `
    UPDATE tesemp
    SET uni_nome = $1
    WHERE cod_uni = $2
    RETURNING *;
  `;

  const client = await conectarPostgres();

  try {
    const res = await client.query(querySql, [uni_nome, cod_uni]);

    res.rows.forEach((retorno) => {
      if (retorno) count += 1;
    });

    console.log("Quantidade de registros atualizados:", count);

    return res.rows;
  } catch (err) {
    console.error("Erro ao atualizar unidade:", err);
    throw err;
  } finally {
    client.release();
  }
}

async function buscarPostgresTeste() {
  let count = 0;
    let querySql = `
      SELECT * 
      FROM tesemp
    `;

    const client = await conectarPostgres();
    const res = await client.query(querySql);
    res.rows.forEach((retorno)=>{
      if(retorno) count += 1;
    })
        console.log('Quantidade de ret?ornos PostgreSqlTESTE: ', count);
        // console.log("Res.rows", res.rows)
    client.release();
    return res.rows;
}

// PostgreSQL
async function buscarPostgres() {
  let count = 0;
    let querySql = `
      SELECT * 
      FROM car_especi
    `;

    const client = await conectarPostgres();
    const res = await client.query(querySql);
    res.rows.forEach((retorno)=>{
      if(retorno) count += 1;
    })
        console.log('Quantidade de retornos PostgreSql: ', count);
    client.release();
    return res.rows;
}

// PostgreSQL2
async function buscarPostgres2(condicoes) {
  let count = 0;
    let querySql = `
      SELECT empnom
      FROM erp_cademp 
      WHERE ${condicoes}
    `;

    const client = await conectarPostgres2();
    const res = await client.query(querySql);
    client.release();
    return res.rows;
}

buscarSQLServer();
buscarPostgres();

module.exports = { buscarSQLServer, buscarPostgresTeste, buscarPostgres, buscarPostgres2, atualizarUnidadeTeste };