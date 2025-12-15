// compararUnidades.js
const { buscarSQLServer, buscarPostgresTeste, buscarPostgres, buscarPostgres2, atualizarUnidadeTeste, atualizarUnidade } = require('./pesquisaSqls');

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

function validaCaracterePelaPosicao(nomeCandidato, posicoes) {
  for (const pos of posicoes) {
    const ch = nomeCandidato[pos];
    // Se não existe caractere nessa posição -> inválido
    if (!ch) return false;
    // Se for letra normal, sem acento -> não pode
    if (/^[A-Za-z]$/.test(ch)) return false;
    // Se for acentuado OU especial -> ok (normalmente é isso)
    if (/[\u00C0-\u017F]/.test(ch)) continue;
    // Caso precise adicionar novas regras, coloca aqui
    // Qualquer coisa fora dos padrões -> inválido
    return false;
  }
  return true;
}

async function gerarCondicoesNome(nomeSql, posicoes) {
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
  return condicoes;
}

// Função para comparar os dados
async function compararUnidades(sqlUnidades, pgUnidades) {
  const unidadesComDiferenca = [];
  const unidadesCorretas = [];
  let count = 0;
  let countCertos = 0;
  let countErrados = 0;

  console.log("PgUnidades: ", pgUnidades)
  console.log("SqlUnidades: ", sqlUnidades)

  // sqlUnidades.forEach(sqlUni => {
  for (const sqlUni of sqlUnidades) {
    count++;

    const pgUni = pgUnidades.find(p => p.coduni === sqlUni.COD_UNI);
    if (pgUni) {
      if (sqlUni.UNI_NOME === pgUni.nomnov) {
        console.log(`Unidade ${sqlUni.COD_UNI}: nomes iguais, nada a fazer.`);
        console.log(`Nome no postgresql: ${pgUni.nomnov}, 
          Nome no sqlServer: ${sqlUni.UNI_NOME}`)
        console.log('---------------------------------------------------------------------------------------------------------------')
        continue;
      }
      // Identifica caracteres corrompidos no SQL Server
      const posCorrompidosSQL = identificarCorrompidos(sqlUni.UNI_NOME);

      if (posCorrompidosSQL.length === 0) {
        // Se não há caracteres corrompidos, apenas log da diferença
        unidadesComDiferenca.push({
          COD_UNI: sqlUni.COD_UNI,
          nomeSQLOriginal: sqlUni.UNI_NOME,
          nomePGOriginal: pgUni.nomnov,
          motivo: 'Diferença encontrada, mas sem caracteres corrompidos no SQL Server'
        });
      }
      // Remove os caracteres corrompidos do SQL Server
      const nomeSqlLimpo = removerPosicoes(sqlUni.UNI_NOME, posCorrompidosSQL);
      // Remove os caracteres nas mesmas posições do PostgreSQL
      const nomePgLimpo = removerPosicoes(pgUni.nomnov, posCorrompidosSQL);
      // Compara os nomes limpos
      if (nomeSqlLimpo !== nomePgLimpo) {
        countErrados++;

        unidadesComDiferenca.push({
          COD_UNI: sqlUni.COD_UNI,
          nomeSQLOriginal: sqlUni.UNI_NOME,
          nomePGOriginal: pgUni.nomnov,
          nomeSQLLimpo: nomeSqlLimpo,
          nomePGLimpo: nomePgLimpo,
          motivo: 'Diferença persiste após remover caracteres corrompidos do SQL Server e equivalentes do PostgreSQL'
        });
      } else {
        // countCertos++;
        let validou = true;
        let motivoErro = '';

        const condicao = await gerarCondicoesNome(pgUni.nomnov, posCorrompidosSQL);
        const nomeEmpresa = await buscarPostgres2(condicao);

        if (!Array.isArray(nomeEmpresa) || nomeEmpresa.length === 0) {
          motivoErro = `Nome da empresa não encontrado, empresa: ${pgUni.nomnov}, código: ${pgUni.coduni}`;
          validou = false;
        }

        if (validou) {
          for (const nome of nomeEmpresa) {
            if (!validaCaracterePelaPosicao(nome.empnom, posCorrompidosSQL)) {
              motivoErro = `Nome da empresa não bateu com os logs, empresa: ${pgUni.nomnov}, código: ${pgUni.coduni}`;
              validou = false;
              break;
            }

            if (nome.empnom.length !== pgUni.nomnov.length) {
              motivoErro = `Tamanho do nome não bateu, empresa: ${pgUni.nomnov}, código: ${pgUni.coduni}`;
              validou = false;
              break;
            }
          }
        }

        if (validou) {
          countCertos++;
          await atualizarUnidade(sqlUni.COD_UNI, pgUni.nomnov);
        } else {
          countErrados++;
          unidadesComDiferenca.push({
            COD_UNI: sqlUni.COD_UNI,
            nomeSQLOriginal: sqlUni.UNI_NOME,
            nomePGOriginal: pgUni.nomnov,
            nomeSQLLimpo: nomeSqlLimpo,
            nomePGLimpo: nomePgLimpo,
            motivo: motivoErro
          });
          console.log(motivoErro);
        }
      }
    }
  };
  return {count, countErrados, countCertos, unidadesComDiferenca};
}

// Função principal para orquestrar
async function verificarUnidades() {
  try {
    const sqlUnidades = (await buscarSQLServer()); // retorna recordset do SQL Server
    const pgUnidades = (await buscarPostgres());       // retorna rows do PostgreSQL

    const resultado = await compararUnidades(sqlUnidades, pgUnidades);

    console.log('Unidades com diferenças após análise:', resultado.unidadesComDiferenca);
    console.log("Total de retornos: (127 esperados)", resultado.count);
    console.log("Total de retornos certos: (127 esperados)", resultado.countCertos);
    console.log("Total de retornos errados: (0 esperados)", resultado.countErrados);

    return resultado;
  } catch (err) {
    console.error('Erro ao verificar unidades:', err);
  }
}

// Executar
verificarUnidades();
