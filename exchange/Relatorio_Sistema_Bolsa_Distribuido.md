# Relatório de Projeto — Sistema Distribuído de Bolsa de Valores com Blockchain de Consórcio

**Grupo H** — Gabriel Castellar · Gustavo El Dib · Leonardo Cico · Pedro Henrique Almeida · Pedro Henrique Teixeira

---

## 1. Visão geral e objetivos

O sistema implementa uma **bolsa de valores com custódia distribuída**, na qual a ordenação e a liquidação das operações não dependem de uma autoridade central única, mas de um conjunto de instituições autorizadas (os bancos/agências) que mantêm conjuntamente um *ledger* replicado em forma de blockchain.

Existem dois papéis no sistema, exatamente como no diagrama de fluxo:

- **Nó menor (cliente/investidor):** envia ordens de compra/venda e recebe atualizações de preço. Não participa do consenso.
- **Nó maior (banco/agência):** recebe ordens, dissemina para os pares, participa do leilão e do consenso, e mantém uma réplica completa da cadeia.

A rede é **permissionada (consórcio):** apenas bancos com identidade criptográfica registrada (um par de chaves) podem produzir e validar blocos. Isso é o que diferencia este projeto de uma blockchain pública aberta — a identidade dos validadores é conhecida *a priori*, o que muda profundamente as escolhas de consenso e de tolerância a falhas.

---

## 2. Estilo arquitetural: microsserviços, monólito ou híbrido?

A resposta honesta é: **arquitetura híbrida — um monólito modular replicado dentro de uma topologia peer-to-peer.** Não é microsserviços puro nem monólito puro, e vale explicar por quê, porque essa é justamente a decisão arquitetural central do projeto.

### 2.1 Por que não é um monólito clássico

Um monólito clássico seria um único processo, um único banco de dados, um único ponto de verdade. Aqui há **seis nós independentes**, distribuídos em duas máquinas físicas, **cada um com seu próprio banco de dados**. Há replicação, particionamento físico e tolerância a falhas — características que um monólito não possui.

### 2.2 Por que não é microsserviços puro

Microsserviços puros implicariam decompor cada nó em serviços finos, independentemente implantáveis e comunicando-se por rede (um serviço de leilão, um de consenso, um de disseminação, um de persistência). Isso seria **contraproducente no caminho crítico do consenso**: o leilão, a verificação e a votação precisam ser determinísticos e de baixa latência. Introduzir saltos de rede entre esses passos dentro de um mesmo nó adicionaria latência e novos modos de falha sem benefício — o gargalo do sistema é a coordenação **entre** os nós, não dentro de um nó.

### 2.3 O que realmente é: monólito modular replicado em P2P

Cada banco é internamente um **monólito modular**, com fronteiras de responsabilidade muito claras:

| Função | Responsabilidade |
|---|---|
| Disseminação | Propagar ordens entre bancos antes do leilão |
| Sincronização | Acordar o conjunto exato de ordens de cada ciclo |
| Consenso | Produzir, verificar, votar e confirmar blocos |
| Leilão | Casamento de ordens e cálculo do preço de equilíbrio |
| Cadeia | Encadeamento por hash, árvore de Merkle e validação |
| Persistência | Réplica local do ledger em banco de dados próprio |
| Criptografia | Assinaturas digitais de blocos e votos |
| Mensageria | Protocolo de comunicação entre os bancos |

Um ponto importante de design: cada nó tem **seu próprio banco de dados**. Esse é o padrão *database-per-service* dos microsserviços, mas aplicado por **nó** em vez de por serviço. É o que garante que não exista um banco compartilhado que seria, ele próprio, um ponto único de falha e de inconsistência.

### 2.4 Recomendação de fronteira (o "híbrido" prático)

A divisão limpa para este projeto é em **duas camadas de implantação por banco**:

1. **Camada de borda / API (voltada ao cliente):** o serviço que recebe ordens do nó menor e devolve a *Atualização de Preço Atual*. Pode escalar horizontalmente, é *stateless* e nunca participa do consenso.
2. **Núcleo de consenso:** o monólito modular replicado, com estado, que faz a disseminação, o leilão, o consenso e mantém a cadeia e o banco de dados local.

Essa separação é o "híbrido" no sentido prático: **microsserviço na borda, monólito modular replicado no núcleo.** A borda absorve picos de clientes; o núcleo, que precisa de determinismo e ordenação global, permanece coeso.

---

## 3. Relação com os quatro pilares de Sistemas Distribuídos

Mapeando diretamente os quatro pontos do livro (Coulouris/Tanenbaum) ao sistema:

### 3.1 Arquitetura — P2P de consórcio permissionado
Topologia totalmente conectada entre os seis bancos: cada nó conhece e se conecta a todos os outros. Não há *master* fixo — a liderança rotaciona entre os bancos (item 3.2). Apenas validadores autorizados, com chave pública conhecida pela rede, entram no consenso.

### 3.2 Coordenação e consenso — ordenação global
A ordenação global acontece em duas etapas encadeadas:
- **Ordenação de mercado (leilão):** calcula, de forma **determinística**, o preço de equilíbrio que maximiza o volume negociado e os negócios resultantes.
- **Ordenação de blocos (consenso):** um líder, escolhido de forma rotativa e previsível a partir do índice do bloco, propõe o bloco; os demais bancos votam. O bloco só é confirmado com a aprovação de um quórum.

### 3.3 Replicação e consistência — *state machine replication*, consistência forte
Cada banco aplica a **mesma sequência determinística de operações** sobre o mesmo estado inicial e chega ao mesmo estado final — a definição de *State Machine Replication*. A consistência é **forte**: um bloco só é aplicado após o acordo do quórum, e cada réplica **revalida o bloco recomputando o leilão localmente** antes de aceitá-lo. Não há aceitação cega da palavra do líder.

### 3.4 Comunicação e segurança — assinaturas e autenticação
Toda mensagem trafega por um protocolo próprio sobre conexões de rede confiáveis. Cada bloco é **assinado digitalmente** e cada réplica verifica a assinatura do produtor contra a chave pública conhecida. O encadeamento por hash, somado à raiz de Merkle das transações, torna a cadeia **à prova de adulteração**: qualquer alteração em um bloco antigo invalidaria todos os blocos seguintes.

---

## 4. Fluxo completo de uma transação (mapeado às mensagens)

As cinco mensagens da especificação correspondem às etapas do fluxo:

| # | Mensagem da especificação | Direção | Papel no fluxo |
|---|---|---|---|
| 1 | Compra/Venda | cliente → banco | Entrada da ordem no sistema |
| 2 | Atualização do Preço Atual | banco → cliente | Resposta de estado de mercado |
| 3 | **Spreading** | banco → todos os bancos | Disseminação da ordem antes do leilão |
| 4 | **Leilão** (proposta + aceite) | entre bancos | Ordenação global e votação |
| 5 | **Propagação de bloco** | banco → todos os bancos | Distribuição do bloco confirmado |

Sequência de ponta a ponta:

1. **Entrada (msg 1):** o cliente envia a ordem e o banco a registra.
2. **Spreading (msg 3):** a ordem é disseminada para todos os bancos *antes* do leilão, para que o conjunto de ordens seja idêntico em todos. (É exatamente aqui que a escolha **Flooding vs Gossip** importa — seção 5.)
3. **Leilão (msg 4):** o líder fecha a janela do ciclo; todos os bancos trocam suas ordens pendentes para acordar o conjunto final; o líder roda o leilão e propõe o bloco; os demais recomputam o resultado e votam. São as duas subfases descritas na especificação: **proposta de ordenação** e **confirmação de aceite**.
4. **Propagação de bloco (msg 5):** com o quórum aprovado, o líder propaga o bloco confirmado e cada réplica o anexa à cadeia e o persiste. Nós que estavam fora do ar se sincronizam pedindo os blocos que perderam.

---

## 5. Disseminação por **Flooding** em vez de **Gossip**

Este é um dos pontos centrais do relatório: como as ordens chegam a todos os bancos antes do leilão.

### 5.1 O que cada um é

- **Gossip (epidêmico):** cada nó, ao receber uma mensagem nova, a repassa para um **subconjunto aleatório de `k` vizinhos** (o *fanout*). A entrega é **probabilística** — converge com altíssima probabilidade, mas não com certeza determinística em uma única rodada.
- **Flooding (broadcast confiável):** cada nó repassa a mensagem nova para **todos os seus vizinhos** (exceto de quem a recebeu). A entrega é **determinística**: enquanto a rede estiver conexa, *toda* mensagem alcança *todos* os nós honestos.

### 5.2 Por que flooding é a escolha correta aqui

A justificativa é o **tamanho e o propósito da rede**:

1. **N é pequeno e fixo (6 bancos, topologia completa).** O custo do flooding é da ordem de N² mensagens por difusão. Com N = 6, isso é desprezível. O argumento clássico a favor do gossip — escalar para milhares de nós com carga de mensagens controlada — **não se aplica** a um consórcio fechado de poucos validadores.

2. **O leilão exige conjuntos de ordens idênticos.** O consenso só aprova um bloco se cada réplica, ao recomputar o leilão, chegar **exatamente aos mesmos negócios** (a comparação é feita por conteúdo das transações, não por identificadores aleatórios). Se um único banco não receber uma ordem por causa da natureza probabilística do gossip, seu leilão local diverge, ele vota pela rejeição, e a rodada é desperdiçada — aumentando a latência. **Flooding elimina essa classe de falha por construção.**

3. **Consistência forte vale mais que economia de banda.** O sistema prioriza consistência forte. Gossip troca garantia por economia de rede; flooding troca um pouco de banda (irrelevante em N = 6) por **garantia determinística de entrega**, que é exatamente o que o consenso precisa.

### 5.3 Trade-offs (registro honesto)

| Critério | Gossip (fanout k) | Flooding (broadcast a todos) |
|---|---|---|
| Garantia de entrega | Probabilística | Determinística (rede conexa) |
| Mensagens por difusão | ~ N·k | ~ N² |
| Escalabilidade (N grande) | Excelente | Ruim |
| Adequação a N pequeno e fixo | Desnecessária | Ideal |
| Risco de divergência no leilão | Existe | Eliminado |

**Decisão:** adotar flooding como modelo padrão da camada de disseminação de ordens, mantendo **deduplicação** — cada nó guarda os identificadores de ordens já vistas e encaminha cada ordem **uma única vez**. A deduplicação é o que torna o flooding seguro, evitando uma tempestade infinita de retransmissões.

---

## 6. Tolerância a falhas bizantinas (BFT)

### 6.1 A diferença entre tolerar *crash* e tolerar comportamento bizantino

Há dois modelos de falha. No modelo **crash-fault**, um nó defeituoso simplesmente para — ele nunca mente. No modelo **bizantino**, um nó pode se comportar de forma **arbitrária ou maliciosa**: enviar informações diferentes para nós diferentes, forjar mensagens, propor blocos inválidos. Uma bolsa de valores entre instituições que **não confiam plenamente umas nas outras** precisa do modelo bizantino, pois um banco comprometido não pode ser capaz de corromper o ledger comum.

Um consenso por **maioria simples** tolera *crash*, mas é **insuficiente** contra um adversário bizantino, por dois motivos:

- **Equivocação:** um líder malicioso pode enviar **blocos diferentes para votantes diferentes** (uma versão para cada metade da rede), conseguindo "maioria" para versões conflitantes ao mesmo tempo.
- **Votos forjáveis:** se os votos não forem assinados, um nó malicioso pode forjar votos em nome de outros bancos.

### 6.2 O que o sistema já tem a favor da BFT

Boa parte das peças fundamentais de um protocolo bizantino já está prevista no projeto:

- **Autenticação e não-repúdio:** blocos assinados digitalmente, com a chave pública de cada produtor conhecida pela rede.
- **Validação independente (propriedade de *validity*):** cada réplica **recomputa o leilão** e compara os negócios resultantes pelo conteúdo das transações, além de revalidar o encadeamento por hash e a raiz de Merkle. Isso já detecta um líder bizantino que tente propor um bloco com negócios inventados ou preços incorretos — o adversário **não consegue** fazer um bloco inválido ser aceito.
- **À prova de adulteração:** encadeamento por hash mais raiz de Merkle das transações.

O que falta é blindar o **processo de votação** contra equivocação e dimensionar corretamente o quórum.

### 6.3 O que falta para ser plenamente BFT (caminho de evolução)

**(a) Dimensionar N e o quórum para o modelo bizantino.**
Para tolerar `f` nós bizantinos é necessário **N ≥ 3f + 1**, com quórum de **2f + 1**.
- Com **N = 6**, o máximo é **f = 1** (pois ⌊(6−1)/3⌋ = 1).
- O quórum bizantino correto passa a ser **4 de 6** — e não a maioria simples de 3.

A mudança conceitual é exigir que a aprovação some **2f + 1 votos válidos e assinados** para o **mesmo** bloco.

**(b) Votos assinados.**
Cada voto deve ser assinado digitalmente sobre o conteúdo (índice, hash do bloco e decisão) e verificado ao ser recebido. Sem isso, não há não-repúdio dos votos.

**(c) Confirmação em múltiplas fases (estilo PBFT) para impedir equivocação.**
Trocar a votação de fase única por três fases:
1. **Pré-preparo** — o líder propõe o bloco.
2. **Preparo** — cada nó difunde (em flooding) que viu a proposta; um nó só avança quando reúne **2f + 1** preparos assinados para o **mesmo** bloco. Isso prova que não existem duas propostas concorrentes aceitas em paralelo.
3. **Confirmação** — cada nó difunde a confirmação; ao reunir **2f + 1** confirmações, aplica o bloco.

As duas subfases que a especificação de mensagens já descreve ("proposta de ordenação" e "confirmação de aceite") encaixam-se naturalmente em pré-preparo/preparo e confirmação.

**(d) Troca de visão para líder bizantino.**
Se o líder do ciclo atual não produz um bloco válido dentro de um tempo limite, ou tenta equivocar, os bancos honestos difundem uma mensagem de troca de visão e avançam para o próximo líder na rotação. Isso garante **vivacidade** (o sistema continua progredindo) mesmo quando o líder é o nó faltoso.

### 6.4 Resumo da postura de segurança

| Propriedade BFT | Estado atual | Ação |
|---|---|---|
| Integridade do bloco | ✅ hash + Merkle + assinatura | manter |
| Validade (recomputar leilão) | ✅ verificação independente | manter |
| Quórum bizantino (2f+1) | ❌ maioria simples | corrigir |
| Votos assinados | ❌ ausente | adicionar |
| Anti-equivocação (3 fases) | ❌ fase única | adicionar |
| Troca de visão p/ líder faltoso | ⚠️ só *crash* | estender p/ bizantino |

Com N = 6 e f = 1, o sistema passa a **continuar correto e disponível mesmo com um banco malicioso ou comprometido** — exatamente a garantia que se espera de uma bolsa em custódia distribuída entre instituições que não confiam plenamente umas nas outras.

---

## 7. Replicação de máquina de estados e consistência forte (detalhe)

O que torna a replicação de máquina de estados viável aqui é o **determinismo total do leilão**. Dado o mesmo conjunto de ordens e o mesmo último preço de fechamento, o leilão produz sempre os mesmos negócios e o mesmo preço de equilíbrio. Por isso a verificação independente funciona: o líder não precisa "provar" seu resultado; cada réplica **reproduz** o cálculo e compara.

Dois cuidados de engenharia sustentam isso:
- **Comparação por conteúdo, não por identidade:** os negócios são comparados pelos atributos econômicos (ativo, ordens de compra e venda envolvidas, quantidade e preço), ignorando identificadores aleatórios. Sem isso, dois leilões determinísticos pareceriam "diferentes" apenas pelos identificadores.
- **Serialização canônica:** o mesmo conteúdo gera sempre exatamente o mesmo hash em qualquer nó, independentemente da ordem em que os campos foram montados.

A consistência é **forte no nível de bloco**: ou todos os nós do quórum têm o bloco N, ou nenhum o aplica. Nós atrasados convergem pedindo os blocos que perderam, sem nunca aceitar um bloco que quebre o encadeamento — qualquer inconsistência de hash ou de índice é rejeitada na anexação.

---

## 8. Roadmap de implementação recomendado

1. **Camada de borda:** expor os pontos de entrada de ordem (msg 1) e de consulta de preço (msg 2), mantendo-os *stateless*.
2. **Disseminação por flooding:** tornar o broadcast completo o padrão da camada de disseminação de ordens, mantendo a deduplicação.
3. **Endurecer o consenso para BFT:**
   - quórum 2f + 1 (4 de 6);
   - assinar e verificar votos;
   - confirmação em três fases (pré-preparo / preparo / confirmação);
   - troca de visão para líder bizantino ou faltoso.
4. **Observabilidade:** métricas de rodadas rejeitadas, latência de bloco e divergências de leilão (que devem cair a zero com flooding).
5. **Testes adversariais:** além dos testes de *crash*, adicionar testes de líder que equivoca e de voto forjado.

---

## 9. Conclusão

O sistema é, em essência, um **monólito modular replicado em uma rede P2P de consórcio permissionado**, com uma **camada de borda em microsserviço** para os clientes — portanto, **híbrido**. Ele já realiza ordenação global determinística (leilão), replicação de máquina de estados e segurança por assinatura digital, atendendo aos quatro pilares de Sistemas Distribuídos.

As duas decisões que diferenciam o projeto de uma blockchain genérica são deliberadas e justificadas pelo contexto de **poucos validadores que não confiam totalmente uns nos outros**:

- **Flooding em vez de Gossip**, porque, com N pequeno, a entrega determinística vale muito mais do que a economia de banda — e porque o leilão exige conjuntos de ordens idênticos em todos os nós.
- **Tolerância bizantina**, porque uma bolsa entre instituições precisa permanecer correta mesmo que um participante aja de forma maliciosa — o que exige quórum 2f + 1, votos assinados e confirmação multifásica anti-equivocação, construídos sobre as garantias de validação determinística e assinatura digital que o sistema já possui.
