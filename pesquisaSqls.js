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

async function atualizarUnidade(cod_uni, uni_nome) {
  const pool = await conectarSQLServer();
  const transaction = new sql.Transaction(pool);

  try {
    await transaction.begin();

    // 1️⃣ SELECT de validação
    const selectReq = new sql.Request(transaction);
    selectReq.input('cod_uni', sql.Int, cod_uni);

    const selectRes = await selectReq.query(`
      SELECT COD_UNI
      FROM AA_Unidades_Empresas
      WHERE COD_UNI = @cod_uni
    `);

    if (selectRes.recordset.length !== 1) {
      console.log('SKIP — cod_uni inválido:', cod_uni);
      await transaction.rollback(); // ⬅️ fecha corretamente
      return;
    }else{
      console.log("Retorno: ", selectRes.recordset, ' - ', cod_uni);
    }

    // 2️⃣ UPDATE
    const updateReq = new sql.Request(transaction);
    updateReq.input('cod_uni', sql.Int, cod_uni);
    updateReq.input('uni_nome', sql.VarChar(255), uni_nome);

    const updateRes = await updateReq.query(`
      UPDATE AA_Unidades_Empresas
      SET UNI_NOME = @uni_nome
      WHERE COD_UNI = @cod_uni;

      SELECT @@ROWCOUNT AS linhas;
    `);

    if (updateRes.recordset[0].linhas !== 1) {
      throw new Error(`Update inválido — afetou ${updateRes.recordset[0].linhas} linhas`);
    }

    // 3️⃣ Commit só se tudo passou
    await transaction.commit();
    console.log(`✔ Update confirmado | COD_UNI=${cod_uni}`);

    // // ❗ rollback forçado para garantir zero impacto
    // await transaction.rollback();
    // console.log('↩️ Rollback executado (modo teste)');

  } catch (err) {
    await transaction.rollback();
    console.error('❌ Erro — rollback executado:', err.message);
    throw err;
  } finally {
    pool.close();
  }
}

// async function atualizarUnidade(cod_uni, uni_nome) {

//   const querySql = `
//     SELECT COD_UNI, UNI_NOME
//     FROM AA_Unidades_Empresas
//     WHERE COD_UNI = @cod_uni
//   `;

//   const pool = await conectarSQLServer();

//   const result = await pool.request()
//     .input('cod_uni', sql.Int, cod_uni)
//     .query(querySql);

//   const count = result.recordset.length;

//   if (count === 0) {
//     throw new Error(`COD_UNI ${cod_uni} não encontrado`);
//   }

//   if (count > 1) {
//     throw new Error(`Erro grave: retornou mais de um registro para COD_UNI ${cod_uni}`);
//   }

//   return result.recordset;
// }


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

module.exports = { buscarSQLServer, buscarPostgresTeste, buscarPostgres, buscarPostgres2, atualizarUnidadeTeste, atualizarUnidade };