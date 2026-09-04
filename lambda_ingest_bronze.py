"""
Lambda de ingestão - camada Bronze
-----------------------------------
Busca séries temporais públicas do Banco Central (API SGS) e grava o JSON
bruto, sem nenhuma transformação, no S3 (camada Bronze).

Variáveis de ambiente esperadas (configure no console da Lambda):
    BUCKET_NAME   -> nome do bucket S3 (ex: "meu-projeto-medalhao")
    SERIES_CODES  -> códigos das séries SGS separados por vírgula
                     (ex: "11,1,433" = Selic diária, câmbio, IPCA)

Trigger recomendado: EventBridge (regra cron, ex: "cron(0 12 * * ? *)"
para rodar todo dia às 12h UTC).

Permissões IAM mínimas para a role de execução:
    - s3:PutObject no bucket/prefixo bronze/
    - logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents
      (já vêm no policy gerenciado AWSLambdaBasicExecutionRole)
"""

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

import boto3

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]
SERIES_CODES = [c.strip() for c in os.environ.get("SERIES_CODES", "11").split(",")]

# API pública do Banco Central - Sistema Gerenciador de Séries Temporais (SGS)
# Desde 26/03/2025 o BCB exige dataInicial/dataFinal para consultas maiores
# (ex: séries diárias como a Selic, código 11); sem isso a API rejeita a
# requisição. Buscamos por padrão os últimos 90 dias a cada execução.
SGS_URL_TEMPLATE = (
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
    "?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
)
DIAS_JANELA = int(os.environ.get("DIAS_JANELA", "90"))


def fetch_series(codigo: str) -> list:
    """Busca uma série do SGS (últimos DIAS_JANELA dias) e retorna data/valor."""
    hoje = datetime.now(timezone.utc)
    inicio = hoje - timedelta(days=DIAS_JANELA)

    url = SGS_URL_TEMPLATE.format(
        codigo=codigo,
        data_inicial=inicio.strftime("%d/%m/%Y"),
        data_final=hoje.strftime("%d/%m/%Y"),
    )
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "aws-medallion-project",
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            raw_body = response.read().decode("utf-8")
            return json.loads(raw_body)
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Falha ao buscar série {codigo}: {exc}") from exc


def build_s3_key(codigo: str, run_date: str) -> str:
    """Organiza o particionamento por série e por data de execução."""
    return f"bronze/serie={codigo}/data_execucao={run_date}/dados.json"


def lambda_handler(event, context):
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    resultados = []

    for codigo in SERIES_CODES:
        dados = fetch_series(codigo)

        payload = {
            "codigo_serie": codigo,
            "data_execucao": run_date,
            "fonte": "BCB-SGS",
            "quantidade_pontos": len(dados),
            "dados": dados,
        }

        key = build_s3_key(codigo, run_date)

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

        resultados.append({"serie": codigo, "s3_key": key, "pontos": len(dados)})

    return {
        "statusCode": 200,
        "body": json.dumps(
            {"mensagem": "Ingestão concluída", "resultados": resultados},
            ensure_ascii=False,
        ),
    }
