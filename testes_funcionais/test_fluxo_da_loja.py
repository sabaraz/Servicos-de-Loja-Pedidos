"""Teste funcional do fluxo da loja, por HTTP, em condicao nominal.

Verifica se cada operacao devolve a resposta correta: sucesso, conflitos,
recurso inexistente, idempotencia, concorrencia, tempo limite e correlacao.
Nao mede desempenho nem injeta falhas; isso e papel dos experimentos.

Por padrao o proprio teste sobe o servico em um processo separado, em uma
porta livre e com um banco temporario descartavel, e o encerra ao final.
Para testar um servico ja em execucao, defina LOJA_ENDERECO_BASE_DO_SERVICO
(ex.: http://127.0.0.1:8000).

Execucao, a partir da raiz do repositorio:
    python -m unittest -v
"""

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

RAIZ_DO_REPOSITORIO = Path(__file__).resolve().parent.parent
ENDERECO_BASE_EXTERNO = os.environ.get("LOJA_ENDERECO_BASE_DO_SERVICO")

TEMPO_LIMITE_PADRAO_EM_SEGUNDOS = 3
TEMPO_LIMITE_DAS_CHAMADAS_CONCORRENTES_EM_SEGUNDOS = 5
TEMPO_LIMITE_CURTO_EM_SEGUNDOS = 0.5
ATRASO_DA_ROTA_LENTA_EM_MILISSEGUNDOS = 2000
TEMPO_MAXIMO_DE_ESPERA_PELO_SERVICO_EM_SEGUNDOS = 20

VARIAVEIS_DE_FALHA_INJETADA = (
    "LOJA_ATRASO_INJETADO_MS",
    "LOJA_PERCENTUAL_DE_FALHAS_503",
    "LOJA_SEMENTE_DA_INJECAO",
)


def _localizarPortaLivreNoComputadorLocal() -> int:
    """Pede ao sistema operacional uma porta TCP livre em 127.0.0.1."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soquete:
        soquete.bind(("127.0.0.1", 0))
        return soquete.getsockname()[1]


def _aguardarServicoResponderOuFalhar(
    endereco_base: str, processo: subprocess.Popen, caminho_do_log: Path
) -> None:
    """Espera o servico aceitar conexoes; falha com o log se ele morrer ou demorar."""
    limite = time.monotonic() + TEMPO_MAXIMO_DE_ESPERA_PELO_SERVICO_EM_SEGUNDOS
    while time.monotonic() < limite:
        if processo.poll() is not None:
            raise RuntimeError(
                "O servico encerrou durante a inicializacao:\n"
                + caminho_do_log.read_text(encoding="utf-8", errors="replace")
            )
        try:
            requests.get(f"{endereco_base}/teste/lento", params={"atraso_ms": 0}, timeout=1)
            return
        except requests.exceptions.ConnectionError:
            time.sleep(0.2)
    raise RuntimeError(
        f"O servico nao respondeu em {TEMPO_MAXIMO_DE_ESPERA_PELO_SERVICO_EM_SEGUNDOS} s."
    )


class TestesFuncionaisDoFluxoDaLoja(unittest.TestCase):
    """Cenarios funcionais da API; cada teste cria os proprios dados."""

    endereco_base: str
    _processo_do_servico: subprocess.Popen | None = None
    _pasta_temporaria: tempfile.TemporaryDirectory | None = None
    _arquivo_de_log_do_servico = None

    # -----------------------------------------------------------------------
    # Ciclo de vida do servico sob teste (nomes impostos pelo unittest)
    # -----------------------------------------------------------------------

    @classmethod
    def setUpClass(cls) -> None:
        if ENDERECO_BASE_EXTERNO:
            cls.endereco_base = ENDERECO_BASE_EXTERNO.rstrip("/")
            return

        cls._pasta_temporaria = tempfile.TemporaryDirectory(prefix="teste_funcional_loja_")
        pasta = Path(cls._pasta_temporaria.name)
        porta = _localizarPortaLivreNoComputadorLocal()
        cls.endereco_base = f"http://127.0.0.1:{porta}"

        ambiente = {
            nome: valor for nome, valor in os.environ.items()
            if nome not in VARIAVEIS_DE_FALHA_INJETADA
        }
        ambiente["LOJA_CAMINHO_DO_BANCO"] = str(pasta / "loja_teste.db")
        ambiente["LOJA_NOME_DA_INSTANCIA"] = "teste-funcional"

        caminho_do_log = pasta / "servico.log"
        cls._arquivo_de_log_do_servico = open(caminho_do_log, "w", encoding="utf-8")
        cls._processo_do_servico = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "servico_da_loja.aplicacao_web:app",
             "--host", "127.0.0.1", "--port", str(porta)],
            cwd=RAIZ_DO_REPOSITORIO,
            env=ambiente,
            stdout=cls._arquivo_de_log_do_servico,
            stderr=subprocess.STDOUT,
        )
        try:
            _aguardarServicoResponderOuFalhar(cls.endereco_base, cls._processo_do_servico, caminho_do_log)
        except Exception:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._processo_do_servico is not None:
            cls._processo_do_servico.terminate()
            cls._processo_do_servico.wait(timeout=10)
            cls._processo_do_servico = None
        if cls._arquivo_de_log_do_servico is not None:
            cls._arquivo_de_log_do_servico.close()
            cls._arquivo_de_log_do_servico = None
        if cls._pasta_temporaria is not None:
            cls._pasta_temporaria.cleanup()
            cls._pasta_temporaria = None

    # -----------------------------------------------------------------------
    # Auxiliares
    # -----------------------------------------------------------------------

    def _enviarRequisicaoComCorrelationId(
        self,
        metodo: str,
        caminho: str,
        tempo_limite: float = TEMPO_LIMITE_PADRAO_EM_SEGUNDOS,
        **argumentos_do_requests,
    ) -> requests.Response:
        """Envia a requisicao com um X-Correlation-ID novo e tempo limite explicito.

        O id enviado aparece nas mensagens de falha, para localizar a mesma
        requisicao no log do servico.
        """
        cabecalhos = argumentos_do_requests.pop("headers", {})
        cabecalhos.setdefault("X-Correlation-ID", f"teste-funcional-{uuid.uuid4().hex[:12]}")
        return requests.request(
            metodo,
            f"{self.endereco_base}{caminho}",
            headers=cabecalhos,
            timeout=tempo_limite,
            **argumentos_do_requests,
        )

    @staticmethod
    def _descreverRespostaParaMensagemDeFalha(resposta: requests.Response) -> str:
        return (
            f"correlation_id={resposta.headers.get('X-Correlation-ID')} "
            f"status={resposta.status_code} corpo={resposta.text[:200]}"
        )

    def _cadastrarProdutoDeTesteComNomeUnico(self, preco: float = 350.0, estoque: int = 5) -> dict:
        resposta = self._enviarRequisicaoComCorrelationId(
            "POST", "/produtos",
            json={"nome": f"produto-teste-{uuid.uuid4().hex[:8]}", "preco": preco, "estoque": estoque},
        )
        self.assertEqual(resposta.status_code, 201, self._descreverRespostaParaMensagemDeFalha(resposta))
        return resposta.json()

    def _abrirPedidoComItem(self, produto_id: int, quantidade: int) -> int:
        resposta_do_pedido = self._enviarRequisicaoComCorrelationId("POST", "/pedidos")
        self.assertEqual(
            resposta_do_pedido.status_code, 201,
            self._descreverRespostaParaMensagemDeFalha(resposta_do_pedido),
        )
        pedido_id = resposta_do_pedido.json()["id"]
        resposta_do_item = self._enviarRequisicaoComCorrelationId(
            "POST", f"/pedidos/{pedido_id}/itens",
            json={"produto_id": produto_id, "quantidade": quantidade},
        )
        self.assertEqual(
            resposta_do_item.status_code, 201,
            self._descreverRespostaParaMensagemDeFalha(resposta_do_item),
        )
        return pedido_id

    def _consultarEstoqueDoProduto(self, produto_id: int) -> int:
        resposta = self._enviarRequisicaoComCorrelationId("GET", f"/produtos/{produto_id}")
        self.assertEqual(resposta.status_code, 200, self._descreverRespostaParaMensagemDeFalha(resposta))
        return resposta.json()["estoque"]

    def _abrirPedidoComChaveCompartilhada(self, chave: str) -> requests.Response:
        return self._enviarRequisicaoComCorrelationId(
            "POST", "/pedidos",
            tempo_limite=TEMPO_LIMITE_DAS_CHAMADAS_CONCORRENTES_EM_SEGUNDOS,
            headers={"Idempotency-Key": chave},
        )

    def _confirmarPedidoCompartilhado(self, pedido_id: int) -> requests.Response:
        return self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido_id}",
            tempo_limite=TEMPO_LIMITE_DAS_CHAMADAS_CONCORRENTES_EM_SEGUNDOS,
            json={"status": "confirmado"},
        )

    # -----------------------------------------------------------------------
    # Produtos
    # -----------------------------------------------------------------------

    def testCadastrarProdutoNovoDevolveCriado201(self) -> None:
        resposta = self._enviarRequisicaoComCorrelationId(
            "POST", "/produtos",
            json={"nome": f"teclado-teste-{uuid.uuid4().hex[:8]}", "preco": 350.0, "estoque": 5},
        )
        self.assertEqual(resposta.status_code, 201, self._descreverRespostaParaMensagemDeFalha(resposta))
        self.assertEqual(resposta.headers.get("Location"), f"/produtos/{resposta.json()['id']}")

    def testCadastrarProdutoComNomeRepetidoDevolveConflito409(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico()
        resposta = self._enviarRequisicaoComCorrelationId(
            "POST", "/produtos", json={"nome": produto["nome"], "preco": 400.0, "estoque": 2},
        )
        self.assertEqual(resposta.status_code, 409, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testBuscarProdutoInexistenteDevolveNaoEncontrado404(self) -> None:
        resposta = self._enviarRequisicaoComCorrelationId("GET", "/produtos/999999")
        self.assertEqual(resposta.status_code, 404, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testRemoverProdutoAssociadoAPedidoDevolveConflito409(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico()
        self._abrirPedidoComItem(produto["id"], quantidade=1)
        resposta = self._enviarRequisicaoComCorrelationId("DELETE", f"/produtos/{produto['id']}")
        self.assertEqual(resposta.status_code, 409, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testRemoverProdutoSemPedidosDevolveSemConteudo204EDeixaDeExistir(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico()
        remocao = self._enviarRequisicaoComCorrelationId("DELETE", f"/produtos/{produto['id']}")
        self.assertEqual(remocao.status_code, 204, self._descreverRespostaParaMensagemDeFalha(remocao))
        consulta = self._enviarRequisicaoComCorrelationId("GET", f"/produtos/{produto['id']}")
        self.assertEqual(consulta.status_code, 404, self._descreverRespostaParaMensagemDeFalha(consulta))

    def testSubstituirProdutoExistenteDevolveEGravaOsDadosNovos(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico(preco=350.0, estoque=5)
        dados_novos = {"nome": f"produto-alterado-{uuid.uuid4().hex[:8]}", "preco": 420.0, "estoque": 9}
        resposta = self._enviarRequisicaoComCorrelationId("PUT", f"/produtos/{produto['id']}", json=dados_novos)
        self.assertEqual(resposta.status_code, 200, self._descreverRespostaParaMensagemDeFalha(resposta))
        self.assertEqual(resposta.json(), {"id": produto["id"], **dados_novos})
        consulta = self._enviarRequisicaoComCorrelationId("GET", f"/produtos/{produto['id']}")
        self.assertEqual(consulta.json(), {"id": produto["id"], **dados_novos})

    def testSubstituirProdutoInexistenteDevolveNaoEncontrado404(self) -> None:
        resposta = self._enviarRequisicaoComCorrelationId(
            "PUT", "/produtos/999999",
            json={"nome": f"produto-fantasma-{uuid.uuid4().hex[:8]}", "preco": 10.0, "estoque": 1},
        )
        self.assertEqual(resposta.status_code, 404, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testSubstituirProdutoComNomeDeOutroProdutoDevolveConflito409(self) -> None:
        primeiro = self._cadastrarProdutoDeTesteComNomeUnico()
        segundo = self._cadastrarProdutoDeTesteComNomeUnico()
        resposta = self._enviarRequisicaoComCorrelationId(
            "PUT", f"/produtos/{segundo['id']}", json={"nome": primeiro["nome"], "preco": 10.0, "estoque": 1},
        )
        self.assertEqual(resposta.status_code, 409, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testCadastrarProdutoComPrecoNegativoDevolveErroDeValidacao422(self) -> None:
        resposta = self._enviarRequisicaoComCorrelationId(
            "POST", "/produtos",
            json={"nome": f"produto-invalido-{uuid.uuid4().hex[:8]}", "preco": -1.0, "estoque": 1},
        )
        self.assertEqual(resposta.status_code, 422, self._descreverRespostaParaMensagemDeFalha(resposta))

    # -----------------------------------------------------------------------
    # Pedidos e itens
    # -----------------------------------------------------------------------

    def testRepetirAberturaComMesmaChaveDeIdempotenciaDevolveMesmoPedido(self) -> None:
        chave = f"pedido-teste-{uuid.uuid4().hex[:8]}"
        primeira = self._enviarRequisicaoComCorrelationId("POST", "/pedidos", headers={"Idempotency-Key": chave})
        repetida = self._enviarRequisicaoComCorrelationId("POST", "/pedidos", headers={"Idempotency-Key": chave})
        self.assertEqual(primeira.status_code, 201, self._descreverRespostaParaMensagemDeFalha(primeira))
        self.assertEqual(repetida.status_code, 200, self._descreverRespostaParaMensagemDeFalha(repetida))
        self.assertEqual(primeira.json()["id"], repetida.json()["id"])

    def testAdicionarMesmoProdutoDuasVezesSomaQuantidadeDoItem(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico(preco=350.0)
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=2)
        resposta = self._enviarRequisicaoComCorrelationId(
            "POST", f"/pedidos/{pedido_id}/itens", json={"produto_id": produto["id"], "quantidade": 2},
        )
        self.assertEqual(resposta.status_code, 200, self._descreverRespostaParaMensagemDeFalha(resposta))
        item = resposta.json()
        self.assertEqual(item["quantidade"], 4)
        self.assertEqual(item["produto_nome"], produto["nome"])
        self.assertEqual(item["subtotal"], 4 * 350.0)

    def testConfirmarPedidoComEstoqueSuficienteDevolveValorTotalPositivo(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico(preco=350.0, estoque=5)
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=2)
        resposta = self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido_id}", json={"status": "confirmado"},
        )
        self.assertEqual(resposta.status_code, 200, self._descreverRespostaParaMensagemDeFalha(resposta))
        consulta = self._enviarRequisicaoComCorrelationId("GET", f"/pedidos/{pedido_id}")
        self.assertEqual(consulta.json()["valor_total"], 700.0)
        self.assertEqual(self._consultarEstoqueDoProduto(produto["id"]), 3)

    def testConfirmarPedidoJaConfirmadoDevolveConflito409(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico()
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=1)
        self._enviarRequisicaoComCorrelationId("PATCH", f"/pedidos/{pedido_id}", json={"status": "confirmado"})
        resposta = self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido_id}", json={"status": "confirmado"},
        )
        self.assertEqual(resposta.status_code, 409, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testConfirmarPedidoComEstoqueInsuficienteDevolveConflito409(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico(estoque=5)
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=999)
        resposta = self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido_id}", json={"status": "confirmado"},
        )
        self.assertEqual(resposta.status_code, 409, self._descreverRespostaParaMensagemDeFalha(resposta))
        self.assertEqual(self._consultarEstoqueDoProduto(produto["id"]), 5, "rollback deveria manter o estoque")

    def testCancelarPedidoConfirmadoDevolveEstoqueEImpedeNovoCancelamento(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico(estoque=5)
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=2)
        self._enviarRequisicaoComCorrelationId("PATCH", f"/pedidos/{pedido_id}", json={"status": "confirmado"})
        cancelamento = self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido_id}", json={"status": "cancelado"},
        )
        self.assertEqual(cancelamento.status_code, 200, self._descreverRespostaParaMensagemDeFalha(cancelamento))
        self.assertEqual(self._consultarEstoqueDoProduto(produto["id"]), 5)
        repeticao = self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido_id}", json={"status": "cancelado"},
        )
        self.assertEqual(repeticao.status_code, 409, self._descreverRespostaParaMensagemDeFalha(repeticao))

    def testCancelarPedidoAbertoDevolveCanceladoSemAlterarEstoque(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico(estoque=5)
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=2)
        resposta = self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido_id}", json={"status": "cancelado"},
        )
        self.assertEqual(resposta.status_code, 200, self._descreverRespostaParaMensagemDeFalha(resposta))
        self.assertEqual(resposta.json()["status"], "cancelado")
        self.assertEqual(self._consultarEstoqueDoProduto(produto["id"]), 5)

    def testConfirmarPedidoSemItensDevolveConflito409(self) -> None:
        pedido = self._enviarRequisicaoComCorrelationId("POST", "/pedidos")
        resposta = self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido.json()['id']}", json={"status": "confirmado"},
        )
        self.assertEqual(resposta.status_code, 409, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testAtualizarPedidoComStatusInvalidoDevolveErroDeValidacao422(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico()
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=1)
        resposta = self._enviarRequisicaoComCorrelationId(
            "PATCH", f"/pedidos/{pedido_id}", json={"status": "entregue"},
        )
        self.assertEqual(resposta.status_code, 422, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testAdicionarItemEmPedidoConfirmadoDevolveConflito409(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico()
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=1)
        self._enviarRequisicaoComCorrelationId("PATCH", f"/pedidos/{pedido_id}", json={"status": "confirmado"})
        resposta = self._enviarRequisicaoComCorrelationId(
            "POST", f"/pedidos/{pedido_id}/itens", json={"produto_id": produto["id"], "quantidade": 1},
        )
        self.assertEqual(resposta.status_code, 409, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testAdicionarItemComProdutoInexistenteDevolveNaoEncontrado404(self) -> None:
        pedido = self._enviarRequisicaoComCorrelationId("POST", "/pedidos")
        resposta = self._enviarRequisicaoComCorrelationId(
            "POST", f"/pedidos/{pedido.json()['id']}/itens", json={"produto_id": 999999, "quantidade": 1},
        )
        self.assertEqual(resposta.status_code, 404, self._descreverRespostaParaMensagemDeFalha(resposta))

    def testListarItensDoPedidoDevolveCadaItemComNomeESubtotal(self) -> None:
        teclado = self._cadastrarProdutoDeTesteComNomeUnico(preco=350.0)
        mouse = self._cadastrarProdutoDeTesteComNomeUnico(preco=100.0)
        pedido_id = self._abrirPedidoComItem(teclado["id"], quantidade=2)
        self._enviarRequisicaoComCorrelationId(
            "POST", f"/pedidos/{pedido_id}/itens", json={"produto_id": mouse["id"], "quantidade": 3},
        )
        resposta = self._enviarRequisicaoComCorrelationId("GET", f"/pedidos/{pedido_id}/itens")
        self.assertEqual(resposta.status_code, 200, self._descreverRespostaParaMensagemDeFalha(resposta))
        itens = {item["produto_nome"]: item["subtotal"] for item in resposta.json()}
        self.assertEqual(itens, {teclado["nome"]: 700.0, mouse["nome"]: 300.0})

    def testListarPedidosIncluiPedidoCriadoComValorTotal(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico(preco=350.0)
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=2)
        resposta = self._enviarRequisicaoComCorrelationId("GET", "/pedidos")
        self.assertEqual(resposta.status_code, 200, self._descreverRespostaParaMensagemDeFalha(resposta))
        pedidos = {pedido["id"]: pedido for pedido in resposta.json()}
        self.assertIn(pedido_id, pedidos)
        self.assertEqual(pedidos[pedido_id]["valor_total"], 700.0)

    def testRotasDePedidoInexistenteDevolvemNaoEncontrado404(self) -> None:
        requisicoes = [
            ("GET", "/pedidos/999999", None),
            ("GET", "/pedidos/999999/itens", None),
            ("POST", "/pedidos/999999/itens", {"produto_id": 1, "quantidade": 1}),
            ("PATCH", "/pedidos/999999", {"status": "confirmado"}),
        ]
        for metodo, caminho, corpo in requisicoes:
            with self.subTest(metodo=metodo, caminho=caminho):
                resposta = self._enviarRequisicaoComCorrelationId(metodo, caminho, json=corpo)
                self.assertEqual(resposta.status_code, 404, self._descreverRespostaParaMensagemDeFalha(resposta))

    # -----------------------------------------------------------------------
    # Concorrencia
    # -----------------------------------------------------------------------

    def testAberturasSimultaneasComMesmaChaveCriamUmUnicoPedido(self) -> None:
        chave = f"pedido-concorrente-{uuid.uuid4().hex[:8]}"
        with ThreadPoolExecutor(max_workers=5) as executor:
            respostas = list(executor.map(lambda _: self._abrirPedidoComChaveCompartilhada(chave), range(5)))
        self.assertEqual(sorted(r.status_code for r in respostas), [200, 200, 200, 200, 201])
        self.assertEqual(len({r.json()["id"] for r in respostas}), 1)

    def testConfirmacoesSimultaneasBaixamEstoqueUmaUnicaVez(self) -> None:
        produto = self._cadastrarProdutoDeTesteComNomeUnico(preco=10.0, estoque=100)
        pedido_id = self._abrirPedidoComItem(produto["id"], quantidade=10)
        with ThreadPoolExecutor(max_workers=2) as executor:
            respostas = list(executor.map(lambda _: self._confirmarPedidoCompartilhado(pedido_id), range(2)))
        self.assertEqual(sorted(r.status_code for r in respostas), [200, 409])
        self.assertEqual(self._consultarEstoqueDoProduto(produto["id"]), 90)

    # -----------------------------------------------------------------------
    # Tempo limite
    # -----------------------------------------------------------------------

    def testTempoLimiteCurtoEmRotaLentaLevantaTimeout(self) -> None:
        with self.assertRaises(requests.exceptions.Timeout):
            self._enviarRequisicaoComCorrelationId(
                "GET", "/teste/lento",
                tempo_limite=TEMPO_LIMITE_CURTO_EM_SEGUNDOS,
                params={"atraso_ms": ATRASO_DA_ROTA_LENTA_EM_MILISSEGUNDOS},
            )

    def testTempoLimiteSuficienteEmRotaLentaDevolveSucesso(self) -> None:
        resposta = self._enviarRequisicaoComCorrelationId(
            "GET", "/teste/lento",
            tempo_limite=TEMPO_LIMITE_PADRAO_EM_SEGUNDOS,
            params={"atraso_ms": ATRASO_DA_ROTA_LENTA_EM_MILISSEGUNDOS},
        )
        self.assertEqual(resposta.status_code, 200, self._descreverRespostaParaMensagemDeFalha(resposta))

    # -----------------------------------------------------------------------
    # Correlacao
    # -----------------------------------------------------------------------

    def testCorrelationIdEnviadoPeloClienteVoltaNaResposta(self) -> None:
        correlation_id = f"teste-correlacao-{uuid.uuid4().hex[:12]}"
        resposta = self._enviarRequisicaoComCorrelationId(
            "GET", "/produtos", headers={"X-Correlation-ID": correlation_id},
        )
        self.assertEqual(resposta.headers.get("X-Correlation-ID"), correlation_id)
        rastreamento = self._enviarRequisicaoComCorrelationId("GET", f"/traces/{correlation_id}")
        self.assertEqual(rastreamento.status_code, 200, self._descreverRespostaParaMensagemDeFalha(rastreamento))

    def testCorrelationIdInvalidoESubstituidoPorUuidGerado(self) -> None:
        resposta = self._enviarRequisicaoComCorrelationId(
            "GET", "/produtos", headers={"X-Correlation-ID": "valor com espacos <invalido>"},
        )
        correlation_id_devolvido = resposta.headers.get("X-Correlation-ID")
        self.assertEqual(str(uuid.UUID(correlation_id_devolvido)), correlation_id_devolvido)


if __name__ == "__main__":
    unittest.main()
