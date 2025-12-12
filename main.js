// compararUnidades.js
const { buscarSQLServer, buscarPostgres } = require('./pesquisaSqls');

// Função para comparar os dados
function compararUnidades(sqlUnidades, pgUnidades) {
  let count = 0;
  const unidadesComDiferenca = [];

  sqlUnidades.forEach(sqlUni => {
    const pgUni = pgUnidades.find(p => p.coduni === sqlUni.COD_UNI);
    if (pgUni) {
      // Verifica se existe caractere de substituição � (65533)
      if (sqlUni.UNI_NOME.includes('�')) {
        count += 1;
        unidadesComDiferenca.push({
          cod_uni: sqlUni.COD_UNI,
          nomeSQL: sqlUni.UNI_NOME,
          nomePG: pgUni.nomnov
        });
      }
    }
  });

  return {count ,unidadesComDiferenca};
}

// Função principal para orquestrar
async function verificarUnidades() {
  try {
    const sqlUnidades = (await buscarSQLServer()); // retorna recordset do SQL Server
    const pgUnidades = (await buscarPostgres());       // retorna rows do PostgreSQL

    const resultado = compararUnidades(sqlUnidades, pgUnidades);

    console.log('Unidades com caracteres problemáticos ou diferença:', resultado.unidadesComDiferenca);
    
    console.log('Quantidade de linhas com defeito(meta: 127):', resultado.count)
    return resultado;

  } catch (err) {
    console.error('Erro ao verificar unidades:', err);
  }
}

// Executar
verificarUnidades();