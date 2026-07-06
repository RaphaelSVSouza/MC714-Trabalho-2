# Projeto LaTeX do relatório

Este diretório contém um projeto LaTeX independente para o relatório do Trabalho 2 da MC714.

## Arquivos

- `main.tex`: arquivo principal do relatório.
- `sections/`: seções do texto separadas por tema.

## Como compilar

Se você tiver uma distribuição LaTeX instalada, rode:

```powershell
pdflatex main.tex
pdflatex main.tex
```

Se preferir `latexmk`, use:

```powershell
latexmk -pdf main.tex
```

O PDF final é gerado como `main.pdf`.