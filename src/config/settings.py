# -----------------------------------------------------
# CONFIGURAÇÕES GERAIS DO PROJETO
# -----------------------------------------------------

# Nome do projeto (útil para logs e paths)
PROJECT_NAME = "mobilidade_urbana_pipeline"

# -----------------------------------------------------
# CKAN - PORTAL DE DADOS ABERTOS BH
# -----------------------------------------------------

CKAN_BASE_URL = "https://dados.pbh.gov.br/api/3/action"

# Resource IDs dos datasets (substituir pelos reais depois)
CKAN_RESOURCES = {
    "onibus_tempo_real": {
        "resource_id": "COLOQUE_O_RESOURCE_ID_AQUI",
        "description": "Posição em tempo real dos ônibus de BH"
    },
    "mco_consolidado": {
        "resource_id": "COLOQUE_O_RESOURCE_ID_AQUI",
        "description": "Mapa de Controle Operacional - dados consolidados"
    }
}

# -----------------------------------------------------
# DATA LAKE - PATHS
# -----------------------------------------------------

DATA_LAKE_BASE_PATH = "/mnt/datalake"

BRONZE_PATH = f"{DATA_LAKE_BASE_PATH}/bronze"
SILVER_PATH = f"{DATA_LAKE_BASE_PATH}/silver"
GOLD_PATH = f"{DATA_LAKE_BASE_PATH}/gold"

# Paths específicos por dataset
BRONZE_DATASETS = {
    "onibus_tempo_real": f"{BRONZE_PATH}/onibus_tempo_real",
    "mco_consolidado": f"{BRONZE_PATH}/mco_consolidado"
}

SILVER_DATASETS = {
    "onibus_tempo_real": f"{SILVER_PATH}/onibus_tempo_real",
    "mco_consolidado": f"{SILVER_PATH}/mco_consolidado"
}

GOLD_DATASETS = {
    "atraso_medio_linha": f"{GOLD_PATH}/atraso_medio_linha",
    "impacto_passageiros": f"{GOLD_PATH}/impacto_passageiros"
}

# -----------------------------------------------------
# PARÂMETROS DE NEGÓCIO
# -----------------------------------------------------

# Definição de horários de pico
PEAK_HOURS = {
    "morning": {
        "start": 6,
        "end": 9
    },
    "evening": {
        "start": 16,
        "end": 19
    }
}

# -----------------------------------------------------
# PARÂMETROS DE INGESTÃO
# -----------------------------------------------------

CKAN_DEFAULT_PAGE_LIMIT = 10000