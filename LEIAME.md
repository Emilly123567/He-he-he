# Ascension Infinite — servidor para o Render (v0.9.2)

Arquivos deste pacote (envie TODOS para a raiz do repositório no GitHub):

    server.mjs        servidor oficial completo (autoritativo, salas, IAs, classificações)
    public/           o jogo (index.html, styles.css, game.js)
    package.json      sem dependências
    render.yaml       configuração do Render
    .node-version     Node 22.22.0
    .gitignore

## 1. GitHub
1. github.com → **New repository** → nome `ascension-infinite` → **Create repository**.
2. Na página do repositório: **uploading an existing file** → arraste o CONTEÚDO desta pasta
   (inclusive a pasta `public` e os arquivos que começam com ponto) → **Commit changes**.
   Se os arquivos com ponto não aparecerem no seu sistema, tudo bem: o Render usa o NODE_VERSION do render.yaml.

## 2. Render
1. render.com → **New** → **Blueprint** → conecte o GitHub e escolha o repositório.
2. Confirme. O Render lê o `render.yaml` e cria o serviço `ascension-infinite`.
   - Manual (alternativa): **New → Web Service** → repositório → Runtime **Node** →
     Build Command `npm install --no-audit --no-fund` → Start Command `node server.mjs` →
     Health Check Path `/api/status` → variável `NODE_VERSION = 22.22.0`.
3. Quando aparecer **Live**, abra `https://SEU-SERVICO.onrender.com` — o jogo conecta sozinho ao
   servidor oficial (wss). Teste: `https://SEU-SERVICO.onrender.com/api/status`.

## Backup local (save unificado)
Cada conta guarda o próprio save NO APARELHO do jogador, o mesmo no online e no offline. Ao entrar,
o jogo envia esse save ao servidor; o que se conquista online volta para o aparelho. Por isso, mesmo
no plano gratuito (que apaga os dados do servidor a cada reinício), **ninguém perde o progresso**:
a conta é recriada com o mesmo id e o save do aparelho. O menu **CONTAS** permite várias contas no
mesmo aparelho e baixar/importar o backup de cada uma em arquivo.

## Plano
- **Free**: dorme após 15 min sem ninguém (o 1º acesso leva ~1 min para acordar). O servidor apaga os
  dados dele a cada reinício (as classificações e as IAs recomeçam), mas o progresso dos jogadores volta
  pelo backup local quando eles entram de novo.
- **Starter + disco**: dados permanentes. No render.yaml: `plan: starter`, descomente `disk` e use `DATA_DIR=/var/data`.

## Variáveis opcionais
- `BOTS=0` desliga as IAs · `BOTS_WARMUP=0,180` antiguidade inicial das IAs em minutos.
- `TRUST_LOCAL_SAVES=0` (modo estrito): só aceita o último save assinado pelo servidor — impede
  progresso editado à mão, mas o progresso feito offline não entra no online.
