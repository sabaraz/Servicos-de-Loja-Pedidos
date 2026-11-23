-- Esquema do banco da loja e dados sinteticos de laboratorio.
-- Seguro para reexecutar a cada inicializacao: CREATE TABLE IF NOT EXISTS
-- nao recria tabelas existentes e INSERT OR IGNORE, junto com UNIQUE(nome),
-- nao duplica os produtos de exemplo.

CREATE TABLE IF NOT EXISTS produtos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL UNIQUE,
    preco REAL NOT NULL CHECK (preco > 0),
    estoque INTEGER NOT NULL CHECK (estoque >= 0)
);

CREATE TABLE IF NOT EXISTS pedidos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL CHECK (status IN ('aberto', 'confirmado', 'cancelado')),
    idempotency_key TEXT UNIQUE,
    criado_em TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS itens_pedido (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pedido_id INTEGER NOT NULL REFERENCES pedidos(id),
    produto_id INTEGER NOT NULL REFERENCES produtos(id),
    quantidade INTEGER NOT NULL CHECK (quantidade > 0),
    preco_unitario REAL NOT NULL,
    UNIQUE(pedido_id, produto_id)
);

-- Produtos iniciais: o fluxo de pedido pode ser exercitado logo apos subir o
-- servico, sem cadastro manual.
INSERT OR IGNORE INTO produtos (nome, preco, estoque) VALUES ('Teclado mecânico', 300, 10);
INSERT OR IGNORE INTO produtos (nome, preco, estoque) VALUES ('Mouse', 100, 15);
INSERT OR IGNORE INTO produtos (nome, preco, estoque) VALUES ('Monitor', 900, 5);
