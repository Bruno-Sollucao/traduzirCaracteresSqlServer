// compararUnidades.js
const { buscarSQLServer, buscarPostgresTeste, buscarPostgres, buscarPostgres2, atualizarUnidadeTeste } = require('./pesquisaSqls');

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

  // sqlUnidades.forEach(sqlUni => {
  for (const sqlUni of sqlUnidades) {
    count++;

    const pgUni = pgUnidades.find(p => p.coduni === sqlUni.cod_uni);

    if (pgUni) {
      // Se os nomes já são iguais, apenas log
      if (sqlUni.uni_nome === pgUni.nomnov) {
        console.log(`Unidade ${sqlUni.cod_uni}: nomes iguais, nada a fazer.`);
        return;
      }
      // Identifica caracteres corrompidos no SQL Server
      const posCorrompidosSQL = identificarCorrompidos(sqlUni.uni_nome);

      if (posCorrompidosSQL.length === 0) {
        // Se não há caracteres corrompidos, apenas log da diferença
        unidadesComDiferenca.push({
          cod_uni: sqlUni.cod_uni,
          nomeSQLOriginal: sqlUni.uni_nome,
          nomePGOriginal: pgUni.nomnov,
          motivo: 'Diferença encontrada, mas sem caracteres corrompidos no SQL Server'
        });
      }
      // Remove os caracteres corrompidos do SQL Server
      const nomeSqlLimpo = removerPosicoes(sqlUni.uni_nome, posCorrompidosSQL);
      // Remove os caracteres nas mesmas posições do PostgreSQL
      const nomePgLimpo = removerPosicoes(pgUni.nomnov, posCorrompidosSQL);
      // Compara os nomes limpos
      if (nomeSqlLimpo !== nomePgLimpo) {
        countErrados++;

        unidadesComDiferenca.push({
          cod_uni: sqlUni.cod_uni,
          nomeSQLOriginal: sqlUni.uni_nome,
          nomePGOriginal: pgUni.nomnov,
          nomeSQLLimpo: nomeSqlLimpo,
          nomePGLimpo: nomePgLimpo,
          motivo: 'Diferença persiste após remover caracteres corrompidos do SQL Server e equivalentes do PostgreSQL'
        });
      } else {
        countCertos++;

        // console.log(`Unidade ${sqlUni.cod_uni}: nomes iguais após limpeza.`);
        // console.log(`Nome limpo PostgreSql: ${nomePgLimpo}`);
        // console.log(`Nome limpo SqlServer:  ${nomeSqlLimpo}`);

        const condicao = await gerarCondicoesNome(pgUni.nomnov, posCorrompidosSQL);

        const nomeEmpresa = await buscarPostgres2(condicao);
        if(Array.isArray(nomeEmpresa) && nomeEmpresa.length > 0){
          nomeEmpresa.forEach(nome =>{
            if (validaCaracterePelaPosicao(nome.empnom, posCorrompidosSQL)) {
              if(nome.empnom.length === pgUni.nomnov.length){
                atualizarUnidadeTeste(sqlUni.cod_uni, pgUni.nomnov)
              }else{
                console.log(`Tamanho do nome não bateu, empresa: ${pgUni.nomnov}, código: ${pgUni.coduni}`)
              }
              // aqui você faz sua lógica final (inserir, atualizar, etc.)
            }else{
              console.log(`Nome da empresa não bateu com os logs, empresa: ${pgUni.nomnov}, código: ${pgUni.coduni}`)
            }
          })
          // console.log("Nome corrigido para incersão: ", nomeEmpresa);
        }else{
          console.log(`Nome da empresa não encontrado, empresa: ${pgUni.nomnov}, código: ${pgUni.coduni}`)
        }
      }
    }
  };
  return {count, countErrados, countCertos, unidadesComDiferenca};
}

// Função principal para orquestrar
async function verificarUnidades() {
  try {
    const sqlUnidades = (await buscarPostgresTeste()); // retorna recordset do SQL Server
    const pgUnidades = (await buscarPostgres());       // retorna rows do PostgreSQL

    const resultado = await compararUnidades(sqlUnidades, pgUnidades);

    console.log('Unidades com diferenças após análise:', resultado.unidadesComDiferenca);
    console.log("Total de retornos: (127 esperados)", resultado.count);
    console.log("Total de retornos: (127 esperados)", resultado.countCertos);
    console.log("Total de retornos: (0 esperados)", resultado.countErrados);

    return resultado;
  } catch (err) {
    console.error('Erro ao verificar unidades:', err);
  }
}

// Executar
verificarUnidades();
