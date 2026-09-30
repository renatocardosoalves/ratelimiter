# Rate Limiter

Biblioteca de **rate limiting** para APIs REST escrita em Go. Ela controla o número de requisições que um cliente pode realizar dentro de uma janela de tempo e, quando o limite é excedido, bloqueia o cliente temporariamente, respondendo `429 Too Many Requests` com o cabeçalho `Retry-After` e (opcionalmente) um JSON estruturado.

- Sem dependências externas (apenas a biblioteca padrão do Go).
- Arquitetura modular: núcleo, middleware HTTP e persistência são componentes independentes e substituíveis.
- Contagem atômica e segura para concorrência.
- Logs estruturados com `log/slog`.

## Sumário

- [Arquitetura](#arquitetura)
- [Algoritmo](#algoritmo)
- [Instalação](#instalação)
- [Uso rápido](#uso-rápido)
- [Parâmetros do rate limiter](#parâmetros-do-rate-limiter-pacote-ratelimiter)
- [Parâmetros do middleware HTTP](#parâmetros-do-middleware-http-pacote-middleware)
- [Parâmetros da persistência em memória](#parâmetros-da-persistência-em-memória-pacote-storage)
- [Resposta quando o limite é excedido](#resposta-quando-o-limite-é-excedido)
- [Logs](#logs)
- [Usando com outros frameworks](#usando-com-outros-frameworks)
- [Criando um novo adaptador de persistência](#criando-um-novo-adaptador-de-persistência)
- [Ambiente de desenvolvimento (Docker)](#ambiente-de-desenvolvimento-docker)
- [API de exemplo](#api-de-exemplo)
- [Teste de carga com hey](#teste-de-carga-com-hey)
- [Testes automatizados](#testes-automatizados)

## Arquitetura

```
.
├── ratelimiter/        # Núcleo: regras, opções, resultado e logs
├── middleware/         # Adaptador HTTP (net/http) + funções de chave do cliente
├── storage/            # Contrato de persistência + adaptador em memória
├── examples/memory/    # API de exemplo com o middleware aplicado
├── Dockerfile          # Imagem de desenvolvimento (golang:1.23-alpine + hey)
└── docker-compose.yaml
```

| Componente | Responsabilidade |
| --- | --- |
| `ratelimiter.Limiter` | Aplica a política (limite, janela, bloqueio), monta o `Result` e emite os logs. Não conhece HTTP. |
| `middleware.RateLimit` | Adaptador HTTP: extrai a chave do cliente, consulta o limiter e escreve a resposta `429`. |
| `storage.Storage` | Adaptador de persistência. Aplica cada requisição de forma **atômica**. |
| `storage.Memory` | Implementação em memória (padrão), particionada em *shards* para reduzir contenção. |

Como o middleware depende apenas da interface `middleware.Limiter` e o limiter depende apenas da interface `storage.Storage`, é possível trocar o servidor HTTP ou a persistência (ex.: Redis) sem alterar o núcleo.

## Algoritmo

É utilizado o algoritmo de **janela fixa com bloqueio**, O(1) em tempo e memória por cliente:

1. A primeira requisição de um cliente abre uma janela de duração `window`.
2. Cada requisição incrementa o contador da janela.
3. Quando o contador ultrapassa `limit`, o cliente é bloqueado por `blockDuration`.
4. Durante o bloqueio **todas** as requisições são rejeitadas (e continuam sendo contadas em `requests_made`).
5. Quando a janela termina, ou quando o bloqueio expira, uma nova janela é iniciada do zero.

A leitura e a atualização do estado de cada cliente acontecem em uma única operação atômica dentro do adaptador de persistência (no adaptador em memória, sob o mutex do *shard* ao qual a chave pertence), então requisições simultâneas nunca geram contagens inconsistentes.

## Instalação

```bash
go get github.com/renatocardosoalves/ratelimiter
```

Requer Go 1.23+.

## Uso rápido

```go
package main

import (
	"log"
	"net/http"

	"github.com/renatocardosoalves/ratelimiter/middleware"
	"github.com/renatocardosoalves/ratelimiter/ratelimiter"
)

func main() {
	// Sem opções: 100 requisições por minuto, 1 minuto de bloqueio, memória.
	limiter, err := ratelimiter.New()
	if err != nil {
		log.Fatal(err)
	}
	defer limiter.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"message":"ok"}`))
	})

	log.Fatal(http.ListenAndServe(":8080", middleware.RateLimit(limiter)(mux)))
}
```

Exemplo com todas as opções configuradas:

```go
store := storage.NewMemory(
	storage.WithShards(512),
	storage.WithCleanupInterval(30*time.Second),
)
defer store.Close()

limiter, err := ratelimiter.New(
	ratelimiter.WithLimit(100),                  // 100 requisições...
	ratelimiter.WithWindow(10*time.Minute),      // ...a cada 10 minutos
	ratelimiter.WithBlockDuration(5*time.Minute),
	ratelimiter.WithStorage(store),
	ratelimiter.WithLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil))),
)
if err != nil {
	log.Fatal(err)
}

handler := middleware.RateLimit(limiter,
	middleware.WithKeyFunc(middleware.KeyByIP),
	middleware.WithJSONResponse(true),
	middleware.WithErrorMessage("Too many requests, slow down"),
	middleware.WithRateLimitHeaders(true),
)(mux)
```

## Parâmetros do rate limiter (pacote `ratelimiter`)

Configurados com o padrão *functional options* em `ratelimiter.New(opts...)`. Qualquer opção omitida recebe o valor padrão. Valores inválidos fazem `New` retornar erro.

| Opção | Descrição | Padrão |
| --- | --- | --- |
| `WithLimit(n int)` | Número máximo de requisições permitidas dentro da janela. Deve ser `> 0`. | `100` |
| `WithWindow(d time.Duration)` | Janela de contagem. Aceita segundos, minutos ou horas: `30*time.Second`, `10*time.Minute`, `time.Hour`. Deve ser `> 0`. | `1m` |
| `WithRate(n int, d time.Duration)` | Atalho para `WithLimit(n)` + `WithWindow(d)`. Ex.: `WithRate(100, 10*time.Minute)`. | — |
| `WithBlockDuration(d time.Duration)` | Tempo que o cliente fica bloqueado após exceder o limite. Deve ser `> 0`. | `1m` |
| `WithStorage(s storage.Storage)` | Adaptador de persistência. Um storage informado aqui **não** é fechado por `Limiter.Close`. | `storage.NewMemory()` |
| `WithLogger(l *slog.Logger)` | Logger estruturado. `nil` desabilita os logs. | `slog.Default()` |
| `WithClock(fn func() time.Time)` | Fonte de tempo (útil em testes). | `time.Now` |

Constantes exportadas: `ratelimiter.DefaultLimit`, `ratelimiter.DefaultWindow`, `ratelimiter.DefaultBlockDuration`.

Métodos:

- `Allow(ctx, key) (Result, error)`: registra uma requisição para a chave e informa se ela pode prosseguir. Útil para integrar com qualquer transporte.
- `Policy() storage.Policy`: retorna a política aplicada.
- `Close() error`: libera o storage em memória criado por padrão.

`ratelimiter.Result`:

| Campo | Descrição |
| --- | --- |
| `Key` | Chave do cliente. |
| `Allowed` | Se a requisição pode prosseguir. |
| `Limit` | Limite configurado. |
| `RequestsMade` | Requisições feitas na janela atual (inclui as rejeitadas). |
| `Remaining` | Requisições restantes na janela atual. |
| `ResetAt` | Fim da janela atual. |
| `RetryAt` | Momento em que o cliente bloqueado poderá tentar novamente (zero quando permitido). |
| `RetryAfter` | Tempo restante até `RetryAt` (zero quando permitido). |

## Parâmetros do middleware HTTP (pacote `middleware`)

`middleware.RateLimit(limiter, opts...)` retorna um `func(http.Handler) http.Handler`.

| Opção | Descrição | Padrão |
| --- | --- | --- |
| `WithKeyFunc(fn KeyFunc)` | Como identificar o cliente. | `KeyByIP` |
| `WithJSONResponse(bool)` | Exibe ou não o corpo JSON na resposta `429`. Quando `false`, apenas status e cabeçalhos são enviados. | `true` |
| `WithErrorMessage(msg string)` | Personaliza o campo `error` do JSON padrão. | `"Rate limit exceeded"` |
| `WithResponseBody[T any](fn func(ratelimiter.Result) T)` | Substitui o JSON padrão por qualquer tipo `T`, serializado em JSON (usa *generics*, sem `interface{}`). Respeita `WithJSONResponse`. | — |
| `WithDeniedHandler(h DeniedHandler)` | Controle total da resposta de bloqueio (status e corpo). Os cabeçalhos `Retry-After`/`X-RateLimit-*` já estão definidos quando ele é chamado. Tem precedência sobre as três opções acima. | — |
| `WithErrorHandler(h ErrorHandler)` | Resposta quando a chave não pode ser extraída ou o limiter falha (ex.: storage indisponível). | `500 Internal Server Error` |
| `WithRateLimitHeaders(bool)` | Envia os cabeçalhos informativos `X-RateLimit-Limit`, `X-RateLimit-Remaining` e `X-RateLimit-Reset` (Unix). `Retry-After` é sempre enviado no `429`. | `true` |

Funções de chave (`KeyFunc`) disponíveis:

| Função | Descrição |
| --- | --- |
| `KeyByIP` | IP da conexão TCP (`RemoteAddr`), sem a porta. Suporta IPv4 e IPv6. |
| `KeyByForwardedIP` | Primeiro IP de `X-Forwarded-For`, depois `X-Real-IP`, depois `RemoteAddr`. **Use apenas atrás de um proxy reverso confiável**, pois esses cabeçalhos podem ser forjados. |
| `KeyByHeader(name)` | Valor de um cabeçalho, ex.: `KeyByHeader("Authorization")`, `KeyByHeader("X-API-Key")` ou `KeyByHeader("User-Agent")`. Retorna `ErrEmptyKey` se ausente. |

Também é possível escrever sua própria função: `func(r *http.Request) (string, error)`.

Exemplo de corpo customizado com generics:

```go
type TooMany struct {
	Message string `json:"message"`
	Wait    int    `json:"wait_seconds"`
}

middleware.RateLimit(limiter,
	middleware.WithResponseBody(func(res ratelimiter.Result) TooMany {
		return TooMany{Message: "Calma aí!", Wait: int(res.RetryAfter.Seconds())}
	}),
)
```

## Parâmetros da persistência em memória (pacote `storage`)

`storage.NewMemory(opts...)`:

| Opção | Descrição | Padrão |
| --- | --- | --- |
| `WithShards(n int)` | Número de partições (arredondado para potência de 2). Mais shards = menos contenção. | `256` |
| `WithCleanupInterval(d)` | Intervalo da limpeza em segundo plano dos registros expirados. `<= 0` desabilita. | `1m` |

Chame `Close()` para encerrar a goroutine de limpeza (feito automaticamente por `Limiter.Close()` quando o storage é o padrão).

## Resposta quando o limite é excedido

```
HTTP/1.1 429 Too Many Requests
Content-Type: application/json; charset=utf-8
Retry-After: 60
X-Ratelimit-Limit: 100
X-Ratelimit-Remaining: 0
X-Ratelimit-Reset: 1738852200
```

```json
{
  "error": "Rate limit exceeded",
  "limit": 100,
  "requests_made": 120,
  "retry_after": "2025-02-06T14:30:00.000Z"
}
```

- `Retry-After`: segundos restantes até o fim do bloqueio (arredondado para cima, mínimo 1).
- `retry_after`: instante exato em que o cliente poderá tentar novamente, em ISO 8601 (UTC, milissegundos).
- `requests_made`: requisições feitas na janela, incluindo as rejeitadas durante o bloqueio.

## Logs

Os logs usam `log/slog` e podem ter qualquer formato (texto, JSON ou um `slog.Handler` próprio) via `WithLogger`.

| Nível | Mensagem | Quando |
| --- | --- | --- |
| `WARN` | `rate limit exceeded` | Uma vez, no momento em que o cliente excede o limite e é bloqueado. |
| `DEBUG` | `request rejected: client blocked` | Cada requisição rejeitada durante o bloqueio. |
| `DEBUG` | `request allowed` | Cada requisição permitida. |

Com o nível padrão do `slog` (`INFO`) apenas o evento de limite excedido é exibido. Campos: `key` (IP do cliente), `requests_made`, `limit`, `window`, `block_duration`, `retry_at`, `retry_after` (e `remaining` nas permitidas).

```
level=WARN msg="rate limit exceeded" key=::1 requests_made=101 limit=100 window=1m0s block_duration=1m0s retry_at=2025-02-06T14:30:00.000Z retry_after=1m0s
```

Para ver todos os eventos:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
limiter, _ := ratelimiter.New(ratelimiter.WithLogger(logger))
```

## Usando com outros frameworks

O middleware segue a assinatura padrão `func(http.Handler) http.Handler`, então funciona diretamente com `net/http`, chi, gorilla/mux e qualquer framework que aceite middlewares `net/http`:

```go
// chi
r := chi.NewRouter()
r.Use(middleware.RateLimit(limiter))

// echo
e.Use(echo.WrapMiddleware(middleware.RateLimit(limiter)))
```

Para outros transportes (gRPC, filas, frameworks sem compatibilidade com `net/http`) basta usar o núcleo diretamente:

```go
res, err := limiter.Allow(ctx, clientKey)
if err != nil { /* storage indisponível */ }
if !res.Allowed { /* rejeitar usando res.RetryAt / res.RetryAfter */ }
```

## Criando um novo adaptador de persistência

Implemente `storage.Storage`:

```go
type Storage interface {
	Hit(ctx context.Context, key string, policy Policy, now time.Time) (Record, error)
}
```

O método deve aplicar a requisição de forma **atômica**. Adaptadores em processo podem reutilizar `Record.Advance(policy, now)`, que contém todo o algoritmo. Adaptadores distribuídos (ex.: Redis) devem reproduzir as mesmas regras no servidor (ex.: script Lua), permitindo compartilhar o limite entre várias instâncias da aplicação:

```go
limiter, _ := ratelimiter.New(ratelimiter.WithStorage(myRedisStorage))
```

## Ambiente de desenvolvimento (Docker)

Pré-requisitos: Docker e Docker Compose (não é necessário ter Go instalado).

O `Dockerfile` (estágio `dev`) usa `golang:1.23-alpine`, instala o [`hey`](https://github.com/rakyll/hey) para testes de carga e mantém o container em execução com `tail -f /dev/null`. O código é montado via *bind mount* em `/app`, então alterações no código não exigem rebuild.

```bash
# Subir o container
docker compose up -d

# Entrar no container
docker compose exec ratelimiter sh

# Derrubar o container
docker compose down
```

## API de exemplo

A API de exemplo (`examples/memory/memory.go`) expõe um único endpoint `GET /` que retorna um JSON de teste e já possui o middleware aplicado.

Dentro do container:

```bash
docker compose exec ratelimiter sh
go run examples/memory/memory.go
```

Flags disponíveis (todas opcionais):

| Flag | Descrição | Padrão |
| --- | --- | --- |
| `-addr` | Endereço HTTP | `:8080` |
| `-limit` | Máximo de requisições por janela | `100` |
| `-window` | Janela de contagem (`30s`, `10m`, `1h`) | `1m` |
| `-block` | Tempo de bloqueio | `1m` |
| `-json` | Exibir o JSON na resposta 429 | `true` |
| `-log-format` | `text` ou `json` | `text` |
| `-debug` | Loga todas as requisições (nível debug) | `false` |

```bash
go run examples/memory/memory.go -limit 10 -window 10s -block 30s -log-format json
```

A porta `8080` também é publicada no host, então é possível testar com `curl http://localhost:8080/` fora do container.

## Teste de carga com hey

Com a API de exemplo rodando em um terminal (passo anterior), abra outro terminal e execute o `hey` dentro do container.

Diretamente via `docker compose exec`:

```bash
# 200 requisições, 50 concorrentes
docker compose exec ratelimiter hey -n 200 -c 50 http://localhost:8080/
```

Ou entrando no container:

```bash
docker compose exec ratelimiter sh
hey -n 200 -c 50 http://localhost:8080/
```

Com a configuração padrão (100 requisições por minuto), o resultado esperado é:

```
Status code distribution:
  [200]	100 responses
  [429]	100 responses
```

E o servidor registra um único log de bloqueio:

```
level=WARN msg="rate limit exceeded" key=::1 requests_made=101 limit=100 window=1m0s block_duration=1m0s ...
```

Outros cenários úteis:

```bash
# Carga contínua por 10 segundos, 100 conexões concorrentes
docker compose exec ratelimiter hey -z 10s -c 100 http://localhost:8080/

# Taxa controlada: 5 workers x 10 req/s = 50 req/s durante 30s
docker compose exec ratelimiter hey -z 30s -c 5 -q 10 http://localhost:8080/

# Ver a resposta 429 completa (cabeçalhos + JSON) enquanto bloqueado
docker compose exec ratelimiter curl -i http://localhost:8080/
```

Observações:

- Todas as requisições feitas de dentro do container chegam com o mesmo IP (`::1`/`127.0.0.1`), portanto são contadas para o mesmo cliente.
- O bloqueio dura o tempo configurado em `-block`; reinicie a API de exemplo para zerar os contadores imediatamente.

## Testes automatizados

```bash
# Todos os testes com detector de condições de corrida
docker compose exec ratelimiter go test -race ./...

# Cobertura
docker compose exec ratelimiter go test -cover ./...

# Benchmarks
docker compose exec ratelimiter go test -run '^$' -bench . ./...

# Análise estática
docker compose exec ratelimiter go vet ./...
```
