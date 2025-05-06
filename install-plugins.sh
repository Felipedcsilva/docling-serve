#!/bin/bash
# Script para instalar plugins adicionais no container docling-server em execução

# Instalação do docling-hybrid-chunking
echo "Instalando docling-hybrid-chunking..."
docker-compose exec docling-server pip install 'git+https://github.com/docling-project/docling-hybrid-chunking.git'

# Verificar instalação
echo "Verificando instalação..."
docker-compose exec docling-server pip list | grep hybrid

echo "Reiniciando serviço docling-server para aplicar mudanças..."
docker-compose restart docling-server

echo "Instalação concluída!" 