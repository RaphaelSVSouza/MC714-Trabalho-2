# Projeto LaTeX do relatório

Este diretório contém o relatório técnico do Trabalho 2 da MC714. O texto está dividido em arquivos por seção e o PDF é gerado a partir de `main.tex`.

## Estrutura

- `main.tex`: preâmbulo, capa, resumo, sumários e inclusão das seções.
- `sections/01-introducao.tex`: contexto, problema e organização do relatório.
- `sections/02-objetivos.tex`: objetivos, requisitos, fundamentação e escopo.
- `sections/03-arquitetura.tex`: componentes, diagramas, mensagens, API e timeouts.
- `sections/04-desenvolvimento.tex`: implementação de Lamport, Ricart-Agrawala e Bully.
- `sections/05-validacao.tex`: testes, experimentos, resultados e ameaças à validade.
- `sections/06-conclusao.tex`: síntese, limitações e extensões.
- `sections/referencias.tex`: bibliografia acadêmica e documentação oficial.

## Compilação

A forma recomendada é:

```powershell
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
```

Alternativamente:

```powershell
pdflatex -interaction=nonstopmode -halt-on-error main.tex
pdflatex -interaction=nonstopmode -halt-on-error main.tex
```

São necessárias pelo menos duas passagens para atualizar sumário, lista de figuras, lista de tabelas e referências cruzadas.

## Limpeza

```powershell
latexmk -C
```

O arquivo final é `main.pdf`. Arquivos auxiliares de compilação são ignorados por `relatorio-latex/.gitignore`.
