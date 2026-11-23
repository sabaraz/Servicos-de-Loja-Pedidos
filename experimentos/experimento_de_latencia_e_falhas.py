"""Experimento de latencia, taxa de erro e throughput do servico da loja.

Diferente do teste funcional (que verifica SE a resposta esta correta), este
script mede QUANTO a operacao demora e COMO falha, sob um cenario controlado.

Procedimento de cada repeticao:
1. sobe o servico em processo separado, porta livre e banco temporario novo,
   com as variaveis de falha injetada do cenario;
2. envia as requisicoes de aquecimento (descartadas da medicao);
3. envia as requisicoes medidas, uma por vez (cliente unico e sequencial),
   cada uma com X-Correlation-ID proprio e tempo limite explicito;
4. classifica cada resultado em sucesso, erro de aplicacao (resposta HTTP de
   erro) ou erro de transporte (sem resposta: conexao recusada ou tempo
   limite);
5. encerra o servico.

Evidencias gravadas em experimentos/<cenario>/:
- resultados.json: ambiente, parametros, hipotese e metricas;
- cliente.log: uma linha JSON por requisicao do cliente;
- servico.log: saida do servico (linhas JSON por requisicao).

Execucao, a partir da raiz do repositorio:
    python experimentos/experimento_de_latencia_e_falhas.py --cenario A-nominal
"""

import argparse
import json
import math
import os
import platform
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

import requests

RAIZ_DO_REPOSITORIO = Path(__file__).resolve().parent.parent
PASTA_DOS_EXPERIMENTOS = Path(__file__).resolve().parent

ROTA_MEDIDA = "/produtos"
QUANTIDADE_PADRAO_DE_AMOSTRAS = 200
QUANTIDADE_PADRAO_DE_REQUISICOES_DE_AQUECIMENTO = 20
QUANTIDADE_PADRAO_DE_REPETICOES = 3
TEMPO_LIMITE_PADRAO_DA_REQUISICAO_EM_SEGUNDOS = 1.0
TEMPO_MAXIMO_DE_ESPERA_PELO_SERVICO_EM_SEGUNDOS = 20
PERCENTIS_REPORTADOS = (50, 95, 99)

BIBLIOTECAS_REGISTRADAS_NO_AMBIENTE = ("fastapi", "starlette", "pydantic", "uvicorn", "requests")

VARIAVEIS_DE_FALHA_INJETADA = (
    "LOJA_ATRASO_INJETADO_MS",
    "LOJA_PERCENTUAL_DE_FALHAS_503",
    "LOJA_SEMENTE_DA_INJECAO",
)

# Cada cenario declara a hipotese ANTES da execucao e as variaveis de ambiente
# que configuram a falha injetada no servico.
CENARIOS_DO_EXPERIMENTO = {
    "A-nominal": {
        "descricao": "Servico sem falha injetada; cliente unico e sequencial; mesma maquina.",
        "hipotese": (
            "Sem falha injetada, 100% das requisicoes medidas tem sucesso, nao ha erro de "
            "aplicacao nem de transporte, o p99 fica abaixo de 50 ms e a media fica proxima "
            "do p50 (cauda curta)."
        ),
        "variaveis_do_servico": {},
    },
}


# ---------------------------------------------------------------------------
# Estatistica
# ---------------------------------------------------------------------------

def calcularPercentilPorPostoMaisProximo(amostras_ordenadas: list[float], percentil: float) -> float | None:
    """Percentil pelo metodo do posto mais proximo (nearest-rank).

    Devolve o menor valor tal que pelo menos `percentil`% das amostras sao
    menores ou iguais a ele: posicao = teto(percentil/100 * n), contada a
    partir de 1. Sempre devolve um valor realmente observado.
    """
    if not amostras_ordenadas:
        return None
    posicao = max(1, math.ceil(percentil / 100 * len(amostras_ordenadas)))
    return amostras_ordenadas[posicao - 1]


def resumirMedicoesDaRepeticao(medicoes: list[dict], duracao_total_em_segundos: float) -> dict:
    """Calcula contagens por resultado, latencia das respostas de sucesso e throughput."""
    latencias_de_sucesso = sorted(m["duration_ms"] for m in medicoes if m["outcome"] == "success")
    erros_de_aplicacao = [m for m in medicoes if m["outcome"] == "application_error"]
    erros_de_transporte = [m for m in medicoes if m["outcome"] == "transport_error"]
    total = len(medicoes)

    resumo = {
        "requisicoes": total,
        "sucessos": len(latencias_de_sucesso),
        "erros_de_aplicacao": len(erros_de_aplicacao),
        "erros_de_aplicacao_por_status": _contarOcorrencias(m["status"] for m in erros_de_aplicacao),
        "erros_de_transporte": len(erros_de_transporte),
        "erros_de_transporte_por_tipo": _contarOcorrencias(m["error"] for m in erros_de_transporte),
        "taxa_de_erro_percentual": round(100 * (total - len(latencias_de_sucesso)) / total, 2) if total else None,
        "duracao_total_s": round(duracao_total_em_segundos, 3),
        "throughput_de_sucessos_por_s": round(len(latencias_de_sucesso) / duracao_total_em_segundos, 2)
        if duracao_total_em_segundos > 0 else None,
        "latencia_de_sucesso_ms": _resumirDistribuicaoDeLatencias(latencias_de_sucesso),
    }
    return resumo


def _resumirDistribuicaoDeLatencias(latencias_ordenadas: list[float]) -> dict:
    if not latencias_ordenadas:
        return {"amostras": 0}
    resumo = {
        "amostras": len(latencias_ordenadas),
        "minimo": round(latencias_ordenadas[0], 2),
        "media": round(statistics.fmean(latencias_ordenadas), 2),
        "desvio_padrao": round(statistics.pstdev(latencias_ordenadas), 2),
        "maximo": round(latencias_ordenadas[-1], 2),
    }
    for percentil in PERCENTIS_REPORTADOS:
        resumo[f"p{percentil}"] = round(calcularPercentilPorPostoMaisProximo(latencias_ordenadas, percentil), 2)
    return resumo


def _contarOcorrencias(valores) -> dict:
    contagem: dict = {}
    for valor in valores:
        contagem[str(valor)] = contagem.get(str(valor), 0) + 1
    return contagem


# ---------------------------------------------------------------------------
# Processo do servico
# ---------------------------------------------------------------------------

def _localizarPortaLivreNoComputadorLocal() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soquete:
        soquete.bind(("127.0.0.1", 0))
        return soquete.getsockname()[1]


def iniciarServicoComFalhaInjetadaDoCenario(
    variaveis_do_servico: dict, caminho_do_banco: Path, arquivo_de_log
) -> tuple[subprocess.Popen, str]:
    """Sobe o servico com as variaveis do cenario; devolve (processo, endereco_base)."""
    porta = _localizarPortaLivreNoComputadorLocal()
    ambiente = {n: v for n, v in os.environ.items() if n not in VARIAVEIS_DE_FALHA_INJETADA}
    ambiente.update(variaveis_do_servico)
    ambiente["LOJA_CAMINHO_DO_BANCO"] = str(caminho_do_banco)
    ambiente["LOJA_NOME_DA_INSTANCIA"] = "servico-experimento"
    processo = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "servico_da_loja.aplicacao_web:app",
         "--host", "127.0.0.1", "--port", str(porta)],
        cwd=RAIZ_DO_REPOSITORIO,
        env=ambiente,
        stdout=arquivo_de_log,
        stderr=subprocess.STDOUT,
    )
    endereco_base = f"http://127.0.0.1:{porta}"
    aguardarServicoAceitarConexoesOuFalhar(endereco_base, processo)
    return processo, endereco_base


def aguardarServicoAceitarConexoesOuFalhar(endereco_base: str, processo: subprocess.Popen) -> None:
    """Espera o servico responder numa rota tecnica (sem falha injetada)."""
    limite = time.monotonic() + TEMPO_MAXIMO_DE_ESPERA_PELO_SERVICO_EM_SEGUNDOS
    while time.monotonic() < limite:
        if processo.poll() is not None:
            raise RuntimeError("O servico encerrou durante a inicializacao; veja servico.log.")
        try:
            requests.get(f"{endereco_base}/teste/lento", params={"atraso_ms": 0}, timeout=1)
            return
        except requests.exceptions.ConnectionError:
            time.sleep(0.2)
    raise RuntimeError(f"O servico nao respondeu em {TEMPO_MAXIMO_DE_ESPERA_PELO_SERVICO_EM_SEGUNDOS} s.")


def encerrarServico(processo: subprocess.Popen) -> None:
    if processo.poll() is None:
        processo.terminate()
        processo.wait(timeout=10)


# ---------------------------------------------------------------------------
# Cliente medidor
# ---------------------------------------------------------------------------

def enviarRequisicaoMedidaEClassificarResultado(
    cliente_http, endereco_base: str, tempo_limite_em_segundos: float
) -> dict:
    """Envia uma requisicao com correlation_id proprio e classifica o resultado.

    - success: resposta HTTP 2xx;
    - application_error: o servico respondeu, mas com status de erro (4xx/5xx);
    - transport_error: nenhuma resposta chegou (conexao recusada ou tempo limite).

    cliente_http e o modulo requests (conexao nova a cada chamada) ou uma
    requests.Session (conexao reaproveitada, keep-alive).
    """
    correlation_id = f"exp-{uuid.uuid4().hex}"
    inicio = time.perf_counter()
    medicao = {"correlation_id": correlation_id, "operation": f"GET {ROTA_MEDIDA}"}
    try:
        resposta = cliente_http.get(
            f"{endereco_base}{ROTA_MEDIDA}",
            headers={"X-Correlation-ID": correlation_id},
            timeout=tempo_limite_em_segundos,
        )
        medicao["status"] = resposta.status_code
        medicao["outcome"] = "success" if resposta.ok else "application_error"
    except requests.exceptions.Timeout:
        medicao["outcome"] = "transport_error"
        medicao["error"] = "timeout"
    except requests.exceptions.ConnectionError:
        medicao["outcome"] = "transport_error"
        medicao["error"] = "connection_error"
    medicao["duration_ms"] = (time.perf_counter() - inicio) * 1000
    return medicao


def registrarMedicaoNoLogDoCliente(arquivo_de_log, medicao: dict, repeticao: int, fase: str) -> None:
    """Grava uma linha JSON por requisicao, com os mesmos campos do log do servico."""
    linha = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": "INFO" if medicao["outcome"] == "success" else "WARNING",
        "service": "cliente_experimento",
        "repeticao": repeticao,
        "fase": fase,
        **{chave: (round(valor, 2) if chave == "duration_ms" else valor) for chave, valor in medicao.items()},
    }
    arquivo_de_log.write(json.dumps(linha, ensure_ascii=False) + "\n")


def executarUmaRepeticaoDoCenario(
    cenario: dict, numero_da_repeticao: int, parametros: dict, log_do_cliente, log_do_servico
) -> dict:
    """Sobe um servico novo, aquece, mede e encerra; devolve o resumo da repeticao."""
    with tempfile.TemporaryDirectory(prefix="experimento_loja_") as pasta_temporaria:
        processo, endereco_base = iniciarServicoComFalhaInjetadaDoCenario(
            cenario["variaveis_do_servico"], Path(pasta_temporaria) / "loja.db", log_do_servico
        )
        cliente_http = requests.Session() if parametros["reaproveitar_conexao"] else requests
        try:
            for _ in range(parametros["aquecimento"]):
                medicao = enviarRequisicaoMedidaEClassificarResultado(
                    cliente_http, endereco_base, parametros["tempo_limite_s"]
                )
                registrarMedicaoNoLogDoCliente(log_do_cliente, medicao, numero_da_repeticao, "aquecimento")

            medicoes = []
            inicio_da_medicao = time.perf_counter()
            for _ in range(parametros["amostras"]):
                medicao = enviarRequisicaoMedidaEClassificarResultado(
                    cliente_http, endereco_base, parametros["tempo_limite_s"]
                )
                registrarMedicaoNoLogDoCliente(log_do_cliente, medicao, numero_da_repeticao, "medicao")
                medicoes.append(medicao)
            duracao_da_medicao = time.perf_counter() - inicio_da_medicao
        finally:
            if cliente_http is not requests:
                cliente_http.close()
            encerrarServico(processo)
    return {"repeticao": numero_da_repeticao, "medicoes": medicoes, "duracao_s": duracao_da_medicao}


# ---------------------------------------------------------------------------
# Ambiente e evidencias
# ---------------------------------------------------------------------------

def coletarDescricaoDoAmbienteDeExecucao() -> dict:
    """Versoes e commit usados, para o relatorio (sem nome de maquina ou usuario)."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=RAIZ_DO_REPOSITORIO,
            capture_output=True, text=True, timeout=5,
        ).stdout.strip() or None
        alteracoes_pendentes = bool(subprocess.run(
            ["git", "status", "--porcelain"], cwd=RAIZ_DO_REPOSITORIO,
            capture_output=True, text=True, timeout=5,
        ).stdout.strip())
    except (OSError, subprocess.SubprocessError):
        commit, alteracoes_pendentes = None, None
    return {
        "sistema_operacional": platform.platform(),
        "python": platform.python_version(),
        "bibliotecas": {nome: metadata.version(nome) for nome in BIBLIOTECAS_REGISTRADAS_NO_AMBIENTE},
        "commit": commit,
        "alteracoes_nao_commitadas": alteracoes_pendentes,
    }


def executarCenarioCompletoEGravarEvidencias(nome_do_cenario: str, parametros: dict) -> dict:
    cenario = CENARIOS_DO_EXPERIMENTO[nome_do_cenario]
    sufixo_da_pasta = "-conexao-reaproveitada" if parametros["reaproveitar_conexao"] else ""
    pasta_do_cenario = PASTA_DOS_EXPERIMENTOS / f"{nome_do_cenario}{sufixo_da_pasta}"
    pasta_do_cenario.mkdir(parents=True, exist_ok=True)
    inicio_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    repeticoes = []
    with open(pasta_do_cenario / "cliente.log", "w", encoding="utf-8") as log_do_cliente, \
         open(pasta_do_cenario / "servico.log", "w", encoding="utf-8") as log_do_servico:
        for numero in range(1, parametros["repeticoes"] + 1):
            repeticoes.append(executarUmaRepeticaoDoCenario(
                cenario, numero, parametros, log_do_cliente, log_do_servico
            ))

    todas_as_medicoes = [m for r in repeticoes for m in r["medicoes"]]
    resultados = {
        "cenario": f"{nome_do_cenario}{sufixo_da_pasta}",
        "descricao": cenario["descricao"],
        "hipotese": cenario["hipotese"],
        "variaveis_do_servico": cenario["variaveis_do_servico"],
        "inicio_utc": inicio_utc,
        "fim_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ambiente": coletarDescricaoDoAmbienteDeExecucao(),
        "parametros": {**parametros, "rota_medida": f"GET {ROTA_MEDIDA}",
                       "percentil": "posto mais proximo (nearest-rank)",
                       "cliente": "unico, sequencial, "
                                  + ("conexao reaproveitada (keep-alive)" if parametros["reaproveitar_conexao"]
                                     else "conexao TCP nova a cada requisicao")},
        "por_repeticao": [
            {"repeticao": r["repeticao"], **resumirMedicoesDaRepeticao(r["medicoes"], r["duracao_s"])}
            for r in repeticoes
        ],
        "agregado": resumirMedicoesDaRepeticao(
            todas_as_medicoes, sum(r["duracao_s"] for r in repeticoes)
        ),
    }
    (pasta_do_cenario / "resultados.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return resultados


def imprimirTabelaResumoDoCenario(resultados: dict) -> None:
    cabecalho = (f"{'Repeticao':<10}{'Req':>6}{'Sucesso':>9}{'Err.apl':>9}{'Err.transp':>11}"
                 f"{'Media':>9}{'p50':>8}{'p95':>8}{'p99':>8}{'Max':>9}{'req/s':>9}")
    print(f"\nCenario {resultados['cenario']}: {resultados['descricao']}")
    print(f"Hipotese: {resultados['hipotese']}\n")
    print(cabecalho)
    linhas = [(str(r["repeticao"]), r) for r in resultados["por_repeticao"]]
    linhas.append(("agregado", resultados["agregado"]))
    for rotulo, resumo in linhas:
        latencia = resumo["latencia_de_sucesso_ms"]
        valores = [latencia.get(c) for c in ("media", "p50", "p95", "p99", "maximo")]
        texto_latencia = "".join(f"{v:>9.2f}" if i in (0, 4) else f"{v:>8.2f}" for i, v in enumerate(valores)) \
            if latencia["amostras"] else f"{'-':>42}"
        print(f"{rotulo:<10}{resumo['requisicoes']:>6}{resumo['sucessos']:>9}{resumo['erros_de_aplicacao']:>9}"
              f"{resumo['erros_de_transporte']:>11}{texto_latencia}"
              f"{resumo['throughput_de_sucessos_por_s'] or 0:>9.1f}")
    print("\nLatencias em ms, apenas respostas de sucesso. Evidencias em experimentos/"
          f"{resultados['cenario']}/")


def lerParametrosDaLinhaDeComando() -> argparse.Namespace:
    leitor = argparse.ArgumentParser(description="Experimento de latencia e falhas do servico da loja.")
    leitor.add_argument("--cenario", required=True, choices=sorted(CENARIOS_DO_EXPERIMENTO))
    leitor.add_argument("--amostras", type=int, default=QUANTIDADE_PADRAO_DE_AMOSTRAS)
    leitor.add_argument("--aquecimento", type=int, default=QUANTIDADE_PADRAO_DE_REQUISICOES_DE_AQUECIMENTO)
    leitor.add_argument("--repeticoes", type=int, default=QUANTIDADE_PADRAO_DE_REPETICOES)
    leitor.add_argument("--tempo-limite", type=float, default=TEMPO_LIMITE_PADRAO_DA_REQUISICAO_EM_SEGUNDOS,
                        help="tempo limite de cada requisicao, em segundos")
    leitor.add_argument("--reaproveitar-conexao", action="store_true",
                        help="usa uma unica conexao keep-alive em vez de uma conexao nova por requisicao")
    return leitor.parse_args()


def executarExperimentoPelaLinhaDeComando() -> None:
    argumentos = lerParametrosDaLinhaDeComando()
    parametros = {
        "amostras": argumentos.amostras,
        "aquecimento": argumentos.aquecimento,
        "repeticoes": argumentos.repeticoes,
        "tempo_limite_s": argumentos.tempo_limite,
        "reaproveitar_conexao": argumentos.reaproveitar_conexao,
    }
    resultados = executarCenarioCompletoEGravarEvidencias(argumentos.cenario, parametros)
    imprimirTabelaResumoDoCenario(resultados)


if __name__ == "__main__":
    executarExperimentoPelaLinhaDeComando()
