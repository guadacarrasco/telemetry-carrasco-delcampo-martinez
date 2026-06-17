# Semana 9: Pipelines de CI/CD

## Objetivos
- Comprender los conceptos de CI/CD (integración continua, despliegue continuo)
- Construir un pipeline con GitHub Actions para pruebas y despliegue automatizados
- Implementar quality gates: lint, tests unitarios y validación de CDK
- Configurar despliegues manuales con selección de entorno

## Herramientas
- GitHub Actions, ruff (lint), pytest (tests), AWS CDK

## Actividad

Crea un pipeline de CI/CD que:
1. Se ejecute en cada push/PR hacia `main` o `project`
2. Valide el código con **ruff** (lint)
3. Ejecute las pruebas unitarias con **pytest** y reporte cobertura
4. Sintetice las plantillas de CDK (`cdk synth`)
5. Permita despliegue manual con selección de entorno (staging / production)

## Estructura de archivos

```
.github/
└── workflows/
    ├── ci.yml          ← se ejecuta automáticamente en push/PR
    └── deploy.yml      ← activación manual desde GitHub Actions

project/
├── requirements.txt        ← dependencias CDK
├── requirements-dev.txt    ← pytest + ruff + boto3
└── tests/
    ├── conftest.py
    └── test_start_simulation.py
```

## Pasos

### 1. Workflow de CI (`.github/workflows/ci.yml`)

El workflow tiene tres jobs que corren en paralelo:

| Job | Qué hace |
|-----|----------|
| `lint` | Instala `ruff` y valida `lambdas/` y `stacks/` |
| `test` | Instala `requirements-dev.txt` y corre `pytest tests/` con cobertura |
| `synth` | Instala CDK, corre `cdk synth` con credenciales dummy |

El workflow se activa solo cuando cambian archivos dentro de `project/`.

### 2. Workflow de despliegue (`.github/workflows/deploy.yml`)

Se activa manualmente con `workflow_dispatch`. Permite elegir entre:
- `staging` — entorno de prueba
- `production` — requiere aprobación del environment en GitHub

Las credenciales AWS se leen de **GitHub Secrets**:
- `AWS_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

Para configurar los secrets: repo → **Settings → Secrets and variables → Actions**.

### 3. Pruebas unitarias (`project/tests/`)

Los tests prueban los handlers de Lambda sin levantar AWS. Se usan mocks con `unittest.mock.patch`:

```python
# Ejemplo: testear que _trigger retorna 400 si faltan campos
def test_missing_session_key():
    event = {"body": json.dumps({"playback_seconds": 60})}
    resp = sim._trigger(event, MagicMock())
    assert resp["statusCode"] == 400
```

`conftest.py` agrega el layer compartido al `sys.path` para que los imports funcionen.

### 4. Agregar ruff al proyecto

```bash
# Verificar lint localmente
cd project
pip install ruff
ruff check lambdas/ stacks/ --exclude lambdas/.build
```

Para ignorar una línea: `# noqa: E501`

## Correr los tests localmente

```bash
cd project
pip install -r requirements-dev.txt
pytest tests/ -v --tb=short --cov=lambdas --cov-report=term-missing
```

## Quality Gates: resumen

| Gate | Herramienta | Cuándo falla |
|------|-------------|--------------|
| Lint | ruff | Código con errores de estilo o bugs estáticos |
| Tests | pytest | Algún test falla o la cobertura baja del umbral |
| Synth | cdk synth | El template CDK tiene errores de configuración |

## Conceptos clave

- **CI**: Construir y testear automáticamente en cada cambio. El objetivo es detectar errores temprano.
- **CD**: Desplegar el código validado a los entornos. Puede ser automático (staging) o manual (production).
- **Quality Gate**: Verificación que debe pasar antes de continuar al siguiente paso del pipeline.
- **Environment Protection**: GitHub permite requerir aprobación manual antes de desplegar a `production`.
- **workflow_dispatch**: Trigger manual de GitHub Actions con parámetros configurables.
- **Secrets**: Variables cifradas en GitHub para credenciales sensibles (nunca en el código).
