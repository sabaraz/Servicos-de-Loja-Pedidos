"""Experimento de latencia, taxa de erro e throughput do servico da loja.

Diferente do teste funcional (que verifica SE a resposta esta correta), este
script mede QUANTO a operacao demora e COMO falha, sob um cenario controlado.

Cada cenario declara, antes da execucao, a hipotese, a perturbacao (variaveis
de falha injetada ou queda do servico) e o criterio de parada.

Procedimento de cada repeticao:
1. sobe o servico em processo separado, porta livre e banco temporario novo,
   com as variaveis de falha injetada do cenario;
2. envia as requisicoes de aquecimento (descartadas da medicao);
3. envia as requisicoes medidas, uma por vez (cliente unico e sequencial),
   cada uma com X-Correlation-ID proprio e tempo limite explicito;
   - se o cenario pede, derruba o servico depois de N requisicoes medidas;
   - interrompe a repeticao se o criterio de parada for atingido;
4. classifica cada resultado em sucesso, erro de aplicacao (o servico
   respondeu com status de erro) ou erro de transporte (nenhuma resposta);
5. aguarda requisicoes ainda em andamento no servico e o encerra.

Ao final, cruza o log do cliente com o log do servico pelo correlation_id para
mostrar o que o servico registrou para cada tipo de resultado do cliente.

Evidencias gravadas em experimentos/<cenario>/<inicio em UTC>/ (uma subpasta
por execucao; execucoes anteriores nunca sao sobrescritas):
- resultados.json: ambiente, parametros, hipotese, metricas e correlacao;
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
PERCENTIS_REPORTADOS = (50, 95, 99)
TEMPO_MAXIMO_DE_ESPERA_PELO_SERVICO_EM_SEGUNDOS = 20
# Tempo dado ao servico para concluir requisicoes que o cliente ja abandonou
# (ex.: por tempo limite) antes de ser encerrado, para que elas cheguem ao log.
TEMPO_DE_DRENAGEM_ANTES_DE_ENCERRAR_EM_SEGUNDOS = 2.0

PARAMETROS_PADRAO = {
    "amostras": 200,
    "aquecimento": 20,
    "repeticoes": 3,
    "tempo_limite_s": 1.0,
    "reaproveitar_conexao": False,
    "derrubar_servico_apos_amostras": None,
    "limite_de_erros_de_transporte_consecutivos": 20,
}

BIBLIOTECAS_REGISTRADAS_NO_AMBIENTE = ("fastapi", "starlette", "pydantic", "uvicorn", "requests")

VARIAVEIS_DE_FALHA_INJETADA = (
    "LOJA_ATRASO_INJETADO_MS",
    "LOJA_PERCENTUAL_DE_FALHAS_503",
    "LOJA_SEMENTE_DA_INJECAO",
)

CRITERIO_DE_PARADA_PADRAO = (
    "Interromper a repeticao apos 20 erros de transporte consecutivos (servico "
    "inalcancavel: continuar so acumularia esperas sem informacao nova)."
)

# Cada cenario declara a hipotese ANTES da execucao, a perturbacao aplicada e
# o criterio de parada. "parametros" substitui os valores de PARAMETROS_PADRAO.
CENARIOS_DO_EXPERIMENTO = {
    "A-nominal": {
        "descricao": "Servico sem falha injetada; cliente unico e sequencial; mesma maquina.",
        "hipotese": (
            "Sem falha injetada, 100% das requisicoes medidas tem sucesso, nao ha erro de "
            "aplicacao nem de transporte, o p99 fica abaixo de 50 ms e a media fica proxima "
            "do p50 (cauda curta)."
        ),
        "perturbacao": "Nenhuma (cenario de referencia).",
        "criterio_de_parada": CRITERIO_DE_PARADA_PADRAO,
        "variaveis_do_servico": {},
        "parametros": {},
    },
    "B-falha-de-aplicacao-503": {
        "descricao": "Servico devolve HTTP 503 em 20% das requisicoes de dominio (sorteio com semente 42).",
        "hipotese": (
            "Entre 15% e 25% das requisicoes medidas terminam em erro de aplicacao 503 e nao ha "
            "erro de transporte. Todo 503 visto pelo cliente aparece no log do servico com o "
            "mesmo correlation_id e injected_fault = true. A latencia dos sucessos fica na mesma "
            "ordem de grandeza do cenario A."
        ),
        "perturbacao": "LOJA_PERCENTUAL_DE_FALHAS_503=20, LOJA_SEMENTE_DA_INJECAO=42.",
        "criterio_de_parada": CRITERIO_DE_PARADA_PADRAO,
        "variaveis_do_servico": {"LOJA_PERCENTUAL_DE_FALHAS_503": "20", "LOJA_SEMENTE_DA_INJECAO": "42"},
        "parametros": {},
    },
    "C-atraso-abaixo-do-tempo-limite": {
        "descricao": "Servico atrasa 300 ms cada resposta de dominio; tempo limite do cliente de 1 s.",
        "hipotese": (
            "Todas as requisicoes medidas tem sucesso (nenhum erro), mas o p50 sobe para cerca de "
            "300 ms mais o p50 do cenario A: a degradacao aparece na latencia, nao na taxa de erro."
        ),
        "perturbacao": "LOJA_ATRASO_INJETADO_MS=300.",
        "criterio_de_parada": CRITERIO_DE_PARADA_PADRAO,
        "variaveis_do_servico": {"LOJA_ATRASO_INJETADO_MS": "300"},
        "parametros": {"amostras": 100, "aquecimento": 5},
    },
    "D-atraso-acima-do-tempo-limite": {
        "descricao": "Servico atrasa 1.200 ms cada resposta de dominio; tempo limite do cliente de 1 s.",
        "hipotese": (
            "100% das requisicoes medidas terminam em erro de transporte por tempo limite de "
            "leitura (read_timeout). Mesmo assim, o servico conclui essas mesmas requisicoes: o log "
            "do servico mostra status 200 e duracao de pelo menos 1.200 ms para os mesmos "
            "correlation_id. O tempo limite nao prova que a operacao deixou de acontecer."
        ),
        "perturbacao": "LOJA_ATRASO_INJETADO_MS=1200.",
        "criterio_de_parada": (
            "Todas as requisicoes devem estourar o tempo limite por desenho, entao o criterio de "
            "erros consecutivos fica desligado; o custo e limitado pela amostra reduzida (30 por "
            "repeticao, cerca de 30 s cada)."
        ),
        "variaveis_do_servico": {"LOJA_ATRASO_INJETADO_MS": "1200"},
        "parametros": {"amostras": 30, "aquecimento": 2, "limite_de_erros_de_transporte_consecutivos": None},
    },
    "E-queda-do-servico": {
        "descricao": "Processo do servico encerrado abruptamente apos 100 requisicoes medidas.",
        "hipotese": (
            "As 100 requisicoes anteriores a queda tem sucesso. As posteriores terminam em erro de "
            "transporte na fase de conexao (connection_error ou connect_timeout), nunca em "
            "read_timeout nem em erro de aplicacao, e nenhuma delas aparece no log do servico. "
            "O criterio de parada interrompe a repeticao apos 20 erros de transporte consecutivos."
        ),
        "perturbacao": "Encerramento do processo do servico (TerminateProcess/SIGTERM) apos 100 amostras.",
        "criterio_de_parada": CRITERIO_DE_PARADA_PADRAO,
        "variaveis_do_servico": {},
        "parametros": {"derrubar_servico_apos_amostras": 100},
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

    return {
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
        "latencia_de_todas_as_requisicoes_ms": _resumirDistribuicaoDeLatencias(
            sorted(m["duration_ms"] for m in medicoes)
        ),
    }


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


def aguardarDrenagemEEncerrarServico(processo: subprocess.Popen) -> None:
    """Da tempo ao servico de concluir requisicoes ja abandonadas pelo cliente e o encerra."""
    if processo.poll() is None:
        time.sleep(TEMPO_DE_DRENAGEM_ANTES_DE_ENCERRAR_EM_SEGUNDOS)
    encerrarServico(processo)


# ---------------------------------------------------------------------------
# Cliente medidor
# ---------------------------------------------------------------------------

def enviarRequisicaoMedidaEClassificarResultado(
    cliente_http, endereco_base: str, tempo_limite_em_segundos: float
) -> dict:
    """Envia uma requisicao com correlation_id proprio e classifica o resultado.

    - success: resposta HTTP 2xx;
    - application_error: o servico respondeu, mas com status de erro (4xx/5xx);
    - transport_error: nenhuma resposta chegou. O tipo distingue a fase:
        connect_timeout  -> a conexao nem foi estabelecida a tempo;
        connection_error -> a conexao foi recusada ou caiu;
        read_timeout     -> a requisicao foi enviada, mas a resposta nao
                            chegou a tempo (o servico pode te-la executado).

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
    # ConnectTimeout herda de ConnectionError e de Timeout: precisa vir antes.
    except requests.exceptions.ConnectTimeout:
        medicao["outcome"] = "transport_error"
        medicao["error"] = "connect_timeout"
    except requests.exceptions.ReadTimeout:
        medicao["outcome"] = "transport_error"
        medicao["error"] = "read_timeout"
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


def registrarEventoDoExperimentoNoLogDoCliente(arquivo_de_log, repeticao: int, evento: str, **dados) -> None:
    """Grava no log do cliente um marco do experimento (queda, interrupcao)."""
    linha = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": "WARNING",
        "service": "cliente_experimento",
        "repeticao": repeticao,
        "evento": evento,
        **dados,
    }
    arquivo_de_log.write(json.dumps(linha, ensure_ascii=False) + "\n")


def _criterioDeParadaFoiAtingido(medicoes: list[dict], limite_de_erros_consecutivos: int | None) -> bool:
    if not limite_de_erros_consecutivos or len(medicoes) < limite_de_erros_consecutivos:
        return False
    ultimas = medicoes[-limite_de_erros_consecutivos:]
    return all(m["outcome"] == "transport_error" for m in ultimas)


def executarUmaRepeticaoDoCenario(
    cenario: dict, numero_da_repeticao: int, parametros: dict, log_do_cliente, log_do_servico
) -> dict:
    """Sobe um servico novo, aquece, mede (com perturbacao e criterio de parada) e encerra."""
    with tempfile.TemporaryDirectory(prefix="experimento_loja_") as pasta_temporaria:
        processo, endereco_base = iniciarServicoComFalhaInjetadaDoCenario(
            cenario["variaveis_do_servico"], Path(pasta_temporaria) / "loja.db", log_do_servico
        )
        cliente_http = requests.Session() if parametros["reaproveitar_conexao"] else requests
        interrompida_pelo_criterio_de_parada = False
        try:
            for _ in range(parametros["aquecimento"]):
                medicao = enviarRequisicaoMedidaEClassificarResultado(
                    cliente_http, endereco_base, parametros["tempo_limite_s"]
                )
                registrarMedicaoNoLogDoCliente(log_do_cliente, medicao, numero_da_repeticao, "aquecimento")

            medicoes = []
            inicio_da_medicao = time.perf_counter()
            for numero_da_amostra in range(parametros["amostras"]):
                if numero_da_amostra == parametros["derrubar_servico_apos_amostras"]:
                    encerrarServico(processo)
                    registrarEventoDoExperimentoNoLogDoCliente(
                        log_do_cliente, numero_da_repeticao, "servico_derrubado",
                        apos_amostras=numero_da_amostra,
                    )
                medicao = enviarRequisicaoMedidaEClassificarResultado(
                    cliente_http, endereco_base, parametros["tempo_limite_s"]
                )
                registrarMedicaoNoLogDoCliente(log_do_cliente, medicao, numero_da_repeticao, "medicao")
                medicoes.append(medicao)
                if _criterioDeParadaFoiAtingido(medicoes, parametros["limite_de_erros_de_transporte_consecutivos"]):
                    interrompida_pelo_criterio_de_parada = True
                    registrarEventoDoExperimentoNoLogDoCliente(
                        log_do_cliente, numero_da_repeticao, "interrompida_pelo_criterio_de_parada",
                        apos_amostras=len(medicoes),
                    )
                    break
            duracao_da_medicao = time.perf_counter() - inicio_da_medicao
        finally:
            if cliente_http is not requests:
                cliente_http.close()
            aguardarDrenagemEEncerrarServico(processo)
    return {
        "repeticao": numero_da_repeticao,
        "medicoes": medicoes,
        "duracao_s": duracao_da_medicao,
        "interrompida_pelo_criterio_de_parada": interrompida_pelo_criterio_de_parada,
    }


# ---------------------------------------------------------------------------
# Correlacao entre o log do cliente e o log do servico
# ---------------------------------------------------------------------------

def _rotularResultadoDoCliente(linha_do_cliente: dict) -> str:
    if linha_do_cliente["outcome"] == "success":
        return "success"
    if linha_do_cliente["outcome"] == "application_error":
        return f"application_error:{linha_do_cliente['status']}"
    return f"transport_error:{linha_do_cliente['error']}"


def _lerLinhasJsonDoArquivo(caminho: Path) -> list[dict]:
    """Le um log e devolve so as linhas JSON (ignora a saida textual do Uvicorn)."""
    linhas = []
    with open(caminho, encoding="utf-8") as arquivo:
        for linha in arquivo:
            if linha.startswith("{"):
                linhas.append(json.loads(linha))
    return linhas


def correlacionarLogDoClienteComLogDoServico(pasta_da_execucao: Path) -> dict:
    """Para cada tipo de resultado visto pelo cliente, mostra o que o servico registrou.

    Usa o correlation_id enviado pelo cliente. Responde, por exemplo: o 503
    recebido foi uma falha injetada? A requisicao que estourou o tempo limite
    foi concluida no servico? A requisicao recusada chegou ao servico?
    """
    linhas_do_servico = {
        linha["correlation_id"]: linha
        for linha in _lerLinhasJsonDoArquivo(pasta_da_execucao / "servico.log")
        if "correlation_id" in linha and "status" in linha
    }
    medicoes_do_cliente = [
        linha for linha in _lerLinhasJsonDoArquivo(pasta_da_execucao / "cliente.log")
        if linha.get("fase") == "medicao"
    ]

    grupos: dict[str, list[dict]] = {}
    for medicao in medicoes_do_cliente:
        grupos.setdefault(_rotularResultadoDoCliente(medicao), []).append(medicao)

    por_resultado = {}
    for rotulo, medicoes in grupos.items():
        no_servico = [linhas_do_servico[m["correlation_id"]] for m in medicoes if m["correlation_id"] in linhas_do_servico]
        por_resultado[rotulo] = {
            "requisicoes_no_cliente": len(medicoes),
            "encontradas_no_log_do_servico": len(no_servico),
            "status_registrado_no_servico": _contarOcorrencias(linha["status"] for linha in no_servico),
            "marcadas_como_falha_injetada_no_servico": sum(1 for linha in no_servico if linha.get("injected_fault")),
            "mediana_da_duracao_no_cliente_ms": round(statistics.median(m["duration_ms"] for m in medicoes), 2),
            "mediana_da_duracao_no_servico_ms": round(statistics.median(linha["duration_ms"] for linha in no_servico), 2)
            if no_servico else None,
            "exemplo_de_correlation_id": medicoes[0]["correlation_id"],
        }
    return {
        "requisicoes_medidas": len(medicoes_do_cliente),
        "encontradas_no_log_do_servico": sum(1 for m in medicoes_do_cliente if m["correlation_id"] in linhas_do_servico),
        "por_resultado_do_cliente": por_resultado,
    }


# ---------------------------------------------------------------------------
# Ambiente e evidencias
# ---------------------------------------------------------------------------

def coletarDescricaoDoAmbienteDeExecucao() -> dict:
    """Versoes e commit usados, para o relatorio (sem nome de maquina ou usuario).

    Fora de um repositorio Git (ex.: projeto copiado sem a pasta .git), commit
    e alteracoes pendentes ficam None (desconhecidos), e nao False.
    """
    commit, alteracoes_pendentes = None, None
    try:
        consulta_do_commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=RAIZ_DO_REPOSITORIO,
            capture_output=True, text=True, timeout=5,
        )
        if consulta_do_commit.returncode == 0:
            commit = consulta_do_commit.stdout.strip() or None
            consulta_de_alteracoes = subprocess.run(
                ["git", "status", "--porcelain", "--", ".", ":(exclude)experimentos/*/*/"],
                cwd=RAIZ_DO_REPOSITORIO, capture_output=True, text=True, timeout=5,
            )
            if consulta_de_alteracoes.returncode == 0:
                alteracoes_pendentes = bool(consulta_de_alteracoes.stdout.strip())
    except (OSError, subprocess.SubprocessError):
        pass
    return {
        "sistema_operacional": platform.platform(),
        "python": platform.python_version(),
        "bibliotecas": {nome: metadata.version(nome) for nome in BIBLIOTECAS_REGISTRADAS_NO_AMBIENTE},
        "commit": commit,
        "alteracoes_de_codigo_nao_commitadas": alteracoes_pendentes,
    }


def resolverParametrosDoCenario(cenario: dict, parametros_da_linha_de_comando: dict) -> dict:
    """Prioridade: linha de comando > parametros do cenario > PARAMETROS_PADRAO."""
    parametros = {**PARAMETROS_PADRAO, **cenario["parametros"]}
    parametros.update({chave: valor for chave, valor in parametros_da_linha_de_comando.items() if valor is not None})
    return parametros


def executarCenarioCompletoEGravarEvidencias(nome_do_cenario: str, parametros_da_linha_de_comando: dict) -> dict:
    cenario = CENARIOS_DO_EXPERIMENTO[nome_do_cenario]
    parametros = resolverParametrosDoCenario(cenario, parametros_da_linha_de_comando)
    nome_completo = f"{nome_do_cenario}{'-conexao-reaproveitada' if parametros['reaproveitar_conexao'] else ''}"

    inicio = datetime.now(timezone.utc)
    pasta_da_execucao = PASTA_DOS_EXPERIMENTOS / nome_completo / inicio.strftime("%Y-%m-%dT%H%M%SZ")
    pasta_da_execucao.mkdir(parents=True, exist_ok=False)
    ambiente = coletarDescricaoDoAmbienteDeExecucao()

    repeticoes = []
    with open(pasta_da_execucao / "cliente.log", "w", encoding="utf-8") as log_do_cliente, \
         open(pasta_da_execucao / "servico.log", "w", encoding="utf-8") as log_do_servico:
        for numero in range(1, parametros["repeticoes"] + 1):
            repeticoes.append(executarUmaRepeticaoDoCenario(
                cenario, numero, parametros, log_do_cliente, log_do_servico
            ))

    todas_as_medicoes = [m for r in repeticoes for m in r["medicoes"]]
    resultados = {
        "cenario": nome_completo,
        "descricao": cenario["descricao"],
        "hipotese": cenario["hipotese"],
        "perturbacao": cenario["perturbacao"],
        "criterio_de_parada": cenario["criterio_de_parada"],
        "variaveis_do_servico": cenario["variaveis_do_servico"],
        "inicio_utc": inicio.isoformat(timespec="seconds"),
        "fim_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ambiente": ambiente,
        "parametros": {**parametros, "rota_medida": f"GET {ROTA_MEDIDA}",
                       "percentil": "posto mais proximo (nearest-rank)",
                       "cliente": "unico, sequencial, "
                                  + ("conexao reaproveitada (keep-alive)" if parametros["reaproveitar_conexao"]
                                     else "conexao TCP nova a cada requisicao")},
        "por_repeticao": [
            {"repeticao": r["repeticao"],
             "interrompida_pelo_criterio_de_parada": r["interrompida_pelo_criterio_de_parada"],
             **resumirMedicoesDaRepeticao(r["medicoes"], r["duracao_s"])}
            for r in repeticoes
        ],
        "agregado": resumirMedicoesDaRepeticao(
            todas_as_medicoes, sum(r["duracao_s"] for r in repeticoes)
        ),
        "correlacao_cliente_servico": correlacionarLogDoClienteComLogDoServico(pasta_da_execucao),
    }
    (pasta_da_execucao / "resultados.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    resultados["pasta_da_execucao"] = pasta_da_execucao.relative_to(RAIZ_DO_REPOSITORIO).as_posix()
    return resultados


def imprimirTabelaResumoDoCenario(resultados: dict) -> None:
    print(f"\nCenario {resultados['cenario']}: {resultados['descricao']}")
    print(f"Hipotese: {resultados['hipotese']}\n")
    print(f"{'Repeticao':<10}{'Req':>6}{'Sucesso':>9}{'Err.apl':>9}{'Err.transp':>11}"
          f"{'Media':>9}{'p50':>9}{'p95':>9}{'p99':>9}{'Max':>9}{'req/s':>8}")
    linhas = [(str(r["repeticao"]) + ("*" if r["interrompida_pelo_criterio_de_parada"] else ""), r)
              for r in resultados["por_repeticao"]]
    linhas.append(("agregado", resultados["agregado"]))
    for rotulo, resumo in linhas:
        latencia = resumo["latencia_de_sucesso_ms"]
        texto_latencia = "".join(f"{latencia[c]:>9.2f}" for c in ("media", "p50", "p95", "p99", "maximo")) \
            if latencia["amostras"] else f"{'-':>45}"
        print(f"{rotulo:<10}{resumo['requisicoes']:>6}{resumo['sucessos']:>9}{resumo['erros_de_aplicacao']:>9}"
              f"{resumo['erros_de_transporte']:>11}{texto_latencia}{resumo['throughput_de_sucessos_por_s'] or 0:>8.1f}")
    print("Latencias em ms, apenas respostas de sucesso. * = interrompida pelo criterio de parada.")

    correlacao = resultados["correlacao_cliente_servico"]
    print(f"\nCorrelacao pelo correlation_id: {correlacao['encontradas_no_log_do_servico']} de "
          f"{correlacao['requisicoes_medidas']} requisicoes medidas encontradas no log do servico.")
    print(f"{'Resultado no cliente':<34}{'Req':>6}{'No servico':>12}{'Status no servico':>22}"
          f"{'Injetada':>10}{'Med.cliente':>13}{'Med.servico':>13}")
    for rotulo, grupo in correlacao["por_resultado_do_cliente"].items():
        mediana_servico = grupo["mediana_da_duracao_no_servico_ms"]
        print(f"{rotulo:<34}{grupo['requisicoes_no_cliente']:>6}{grupo['encontradas_no_log_do_servico']:>12}"
              f"{json.dumps(grupo['status_registrado_no_servico']):>22}"
              f"{grupo['marcadas_como_falha_injetada_no_servico']:>10}"
              f"{grupo['mediana_da_duracao_no_cliente_ms']:>13.2f}"
              f"{(f'{mediana_servico:.2f}' if mediana_servico is not None else '-'):>13}")
    print(f"\nEvidencias em {resultados['pasta_da_execucao']}/")


def lerParametrosDaLinhaDeComando() -> argparse.Namespace:
    leitor = argparse.ArgumentParser(description="Experimento de latencia e falhas do servico da loja.")
    leitor.add_argument("--cenario", required=True, choices=sorted(CENARIOS_DO_EXPERIMENTO))
    leitor.add_argument("--amostras", type=int, help="requisicoes medidas por repeticao")
    leitor.add_argument("--aquecimento", type=int, help="requisicoes descartadas por repeticao")
    leitor.add_argument("--repeticoes", type=int, help="repeticoes, cada uma com servico novo")
    leitor.add_argument("--tempo-limite", type=float, help="tempo limite de cada requisicao, em segundos")
    leitor.add_argument("--reaproveitar-conexao", action="store_true", default=None,
                        help="usa uma unica conexao keep-alive em vez de uma conexao nova por requisicao")
    return leitor.parse_args()


def executarExperimentoPelaLinhaDeComando() -> None:
    argumentos = lerParametrosDaLinhaDeComando()
    parametros_da_linha_de_comando = {
        "amostras": argumentos.amostras,
        "aquecimento": argumentos.aquecimento,
        "repeticoes": argumentos.repeticoes,
        "tempo_limite_s": argumentos.tempo_limite,
        "reaproveitar_conexao": argumentos.reaproveitar_conexao,
    }
    resultados = executarCenarioCompletoEGravarEvidencias(argumentos.cenario, parametros_da_linha_de_comando)
    imprimirTabelaResumoDoCenario(resultados)


if __name__ == "__main__":
    executarExperimentoPelaLinhaDeComando()
