// compararUnidades.js
const { buscarSQLServer, buscarPostgres, buscarPostgres2 } = require('./pesquisaSqls');

// Função para identificar caracteres corrompidos (�) no SQL Server
function identificarCorrompidos(str) {
  const posicoes = [];
  for (let i = 0; i < str.length; i++) {
    if (str[i].charCodeAt(0) === 65533) { // �
      posicoes.push(i);
    }
  }
  return posicoes;
}

// Função para remover caracteres em posições específicas
function removerPosicoes(str, posicoes) {
  return str
    .split('')
    .filter((_, idx) => !posicoes.includes(idx))
    .join('');
}

// 🔥 Função que quebra o nome em partes e monta condições ILIKE
function gerarCondicoesNome(nomeSql, posicoes) {
    // Garantir que as posições estão ordenadas
    posicoes = [...posicoes].sort((a, b) => a - b);

    let partes = [];
    let inicio = 0;

    posicoes.forEach(pos => {
    // Pegamos o trecho entre 'inicio' e a posição atual
        if (pos > inicio) {
            partes.push(nomeSql.substring(inicio, pos));
        }
    // Atualiza o início ignorando o caractere corrompido
        inicio = pos + 1;
    });

  // Último trecho após o final da última posição
    if (inicio < nomeSql.length) {
        partes.push(nomeSql.substring(inicio));
    }

  // gera → AND nome ILIKE '%parte%'
    const condicoes = partes.map(p => `empnom ILIKE '%${p}%'`).join(" AND ");

    return { partes, condicoes };
}

// Função para comparar os dados
async function compararUnidades(sqlUnidades, pgUnidades) {
  const unidadesComDiferenca = [];
  const unidadesCorretas = [];

  const acertosPorSql = [];
  const errosPorSql = [];

  let count = 0;
  let countCertos = 0;
  let countErrados = 0;

  let encontrado;

  for (const sqlUni of sqlUnidades) {
    count++;
    let pgUni = pgUnidades.find(p => p.coduni === sqlUni.COD_UNI);

    console.log("Pg:", pgUni)

    if (pgUni) {
      console.log(`\n❗ Não achou ${sqlUni.COD_UNI} no PG pelo código. Tentando pelo nome...`);

      const posCorrompidosSQL = identificarCorrompidos(sqlUni.UNI_NOME);

      const { partes, condicoes } = gerarCondicoesNome(sqlUni.UNI_NOME, posCorrompidosSQL);

      console.log("Partes encontradas:", partes);
      console.log("Condições geradas:", condicoes);

      encontrado = await buscarPostgres2(condicoes);
    }


    // Se os nomes já são iguais
    if (sqlUni.UNI_NOME === encontrado.empnov) {
      countCertos++;
      acertosPorSql.push({ sql: sqlUni, pg: pgUni });
      continue;
    }

    console.log("Pg:", pgUni)

    // Identificar corrompidos
    const posCorrompidosSQL = identificarCorrompidos(sqlUni.UNI_NOME);

    if (posCorrompidosSQL.length === 0) {
      unidadesComDiferenca.push({
        cod_uni: sqlUni.COD_UNI,
        nomeSQLOriginal: sqlUni.UNI_NOME,
        nomePGOriginal: pgUni.nomnov,
        motivo: 'Diferença encontrada, mas sem caracteres corrompidos no SQL Server'
      });
      countErrados++;
      errosPorSql.push({ sql: sqlUni, pg: pgUni });
      continue;
    }

    console.log("Pg:", pgUni)

    console.log(`sql: ${sqlUni.UNI_NOME}`)
    console.log(`pg: ${pgUni.nomnov}`)

    // Limpar
    const nomeSqlLimpo = removerPosicoes(sqlUni.UNI_NOME, posCorrompidosSQL);
    const nomePgLimpo = removerPosicoes(pgUni.nomnov, posCorrompidosSQL);



    // Comparar
    if (nomeSqlLimpo !== nomePgLimpo) {
      countErrados++;
      errosPorSql.push({ sql: sqlUni, pg: pgUni });

      unidadesComDiferenca.push({
        cod_uni: sqlUni.COD_UNI,
        nomeSQLOriginal: sqlUni.UNI_NOME,
        nomePGOriginal: pgUni.nomnov,
        nomeSQLLimpo: nomeSqlLimpo,
        nomePGLimpo: nomePgLimpo,
        motivo: 'Diferença persiste após limpeza'
      });
    } else {
      countCertos++;
      acertosPorSql.push({ sql: sqlUni, pg: pgUni });
    }
  }

  return {
    count,
    countCertos,
    countErrados,
    acertosPorSql,
    errosPorSql,
    unidadesComDiferenca
  };
}

// Função principal
async function verificarUnidades() {
  try {
    const sqlUnidades = await buscarSQLServer();
    const pgUnidades = await buscarPostgres();

    const resultado = await compararUnidades(sqlUnidades, pgUnidades);

    // console.log('\n=== RESULTADO FINAL ===');
    // console.log("Total SQL:", resultado.count);
    // console.log("Certos:", resultado.countCertos);
    // console.log("Errados:", resultado.countErrados);

    return resultado;
  } catch (err) {
    console.error('Erro ao verificar unidades:', err);
  }
}

verificarUnidades();