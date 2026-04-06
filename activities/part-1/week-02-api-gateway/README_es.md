# Semana 2: API Gateway y Event Sources

## Objetivos
- Comprender los conceptos de REST API en API Gateway
- Exponer funciones Lambda mediante endpoints HTTP
- Manejar parámetros de solicitud, variables de ruta y cuerpos de solicitud
- Implementar códigos de estado HTTP y manejo de errores adecuados

## Herramientas
- SAM CLI, API Gateway, Lambda, Python 3.12

## Actividad
Exponer la simulación como una REST API con los siguientes endpoints:
- `GET /sessions` — Listar todas las sesiones de carrera de OpenF1 (temporada 2024)
- `GET /sessions/{session_key}` — Obtener los detalles de una sesión
- `POST /sessions/{session_key}/ingest` — Disparar la ingesta de datos desde OpenF1

## Pasos

### 1. Revisa el código inicial
El starter incluye:
- `handler.py` — tres esqueletos de handlers con comentarios TODO
- `template.yaml` — template de SAM con una función definida y dos más para agregar
- `requirements.txt` — dependencias de Python

### 2. Entiende el formato del evento de API Gateway

Cuando API Gateway invoca tu Lambda, el objeto `event` se ve así:

```json
{
  "httpMethod": "GET",
  "path": "/sessions/9158",
  "pathParameters": {
    "session_key": "9158"
  },
  "queryStringParameters": {
    "year": "2024"
  },
  "body": null,
  "headers": {
    "Content-Type": "application/json"
  }
}
```

Campos clave:
- **Path parameters**: `event["pathParameters"]["session_key"]` — valores de los `{placeholders}` en la URL
- **Query parameters**: `event.get("queryStringParameters") or {}` — valores después del `?` en la URL (puede ser `None`)
- **Cuerpo de la solicitud**: `json.loads(event["body"])` — para solicitudes POST/PUT (string que necesita ser parseado)

### 3. Implementa los handlers

**`list_sessions`** — Llama a la API de OpenF1 para obtener sesiones de carrera. Usa los query parameters `session_type=Race` y `year=2024` para filtrar resultados. Retorna una lista con la key, nombre, tipo, circuito y fecha de inicio de cada sesión.

**`get_session`** — Extrae `session_key` de los path parameters. Llama a la API de OpenF1 con esa key. Retorna 404 si la sesión no se encuentra.

**`ingest_session`** — Extrae `session_key` de los path parameters. Obtiene tanto la sesión como sus pilotos desde OpenF1. Retorna un resumen con el nombre de la sesión y la lista de pilotos encontrados. Este endpoint hace más trabajo — necesitará un timeout más largo (ver paso 4).

Los tres handlers deben:
- Incluir `{"Content-Type": "application/json"}` en los headers de respuesta
- Retornar códigos de estado apropiados (200, 404, 500)
- Capturar `requests.RequestException` y retornar 500 con el mensaje de error

### 4. Configura API Gateway en SAM

El template del starter tiene `ListSessionsFunction` completamente definida. Necesitas agregar dos funciones más siguiendo el mismo patrón:

- **GetSessionFunction**: handler `handler.get_session`, path `/sessions/{session_key}`, method `GET`
- **IngestSessionFunction**: handler `handler.ingest_session`, path `/sessions/{session_key}/ingest`, method `POST`

Nota la sintaxis `{session_key}` en el path — así es como SAM define los path parameters. API Gateway extraerá el valor y lo pasará en `event["pathParameters"]`.

> **Tip**: El endpoint de ingesta obtiene más datos y puede tardar más. Considera establecer un `Timeout` más alto (ej: 120 segundos) y `MemorySize` mayor (ej: 512 MB) para esa función.

### 5. Compila y prueba localmente

SAM usa Docker para emular API Gateway y Lambda localmente. Debes compilar antes de ejecutar:

```bash
sam build
sam local start-api
```

Luego en otra terminal:
```bash
# Listar todas las sesiones de carrera 2024
curl http://localhost:3000/sessions

# Obtener una sesión específica
curl http://localhost:3000/sessions/9158

# Disparar ingesta
curl -X POST http://localhost:3000/sessions/9158/ingest
```

> **Nota**: La primera invocación puede ser lenta porque Docker descarga la imagen de Lambda Python 3.12. Las siguientes llamadas son más rápidas.

### 6. Desplegar (opcional)
```bash
sam build
sam deploy --guided
```

## Resultados esperados
- `GET /sessions` retorna un array JSON de sesiones de carrera
- `GET /sessions/9158` retorna los detalles de una sesión
- `GET /sessions/0000` retorna `{"error": "Session not found"}` con status 404
- `POST /sessions/9158/ingest` retorna el nombre de la sesión y una lista de pilotos
- Todas las respuestas de error incluyen códigos de estado y mensajes apropiados

## Conceptos clave
- **REST API**: URLs basadas en recursos con métodos HTTP (GET, POST, PUT, DELETE)
- **Evento de API Gateway**: El objeto event que Lambda recibe de API Gateway — incluye path parameters, query strings, headers y body
- **Path Parameters**: `/sessions/{session_key}` → `event["pathParameters"]["session_key"]`
- **Lambda Proxy Integration**: API Gateway pasa la solicitud HTTP completa a Lambda y espera una respuesta con `statusCode`, `headers` y `body`
- **CORS**: Headers de Cross-Origin Resource Sharing para acceso desde el navegador

## Referencia de la API OpenF1
Estos son los endpoints que llamarás desde tus handlers:

| Endpoint | Descripción |
|----------|-------------|
| `GET /v1/sessions?session_type=Race&year=2024` | Listar sesiones de carrera |
| `GET /v1/sessions?session_key=9158` | Obtener una sesión específica |
| `GET /v1/drivers?session_key=9158` | Obtener pilotos de una sesión |

URL base: `https://api.openf1.org`

## Recursos
- [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/latest/dg/)
- [API Gateway REST API](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-rest-api.html)
- [OpenF1 API Documentation](https://openf1.org/)
- [SAM CLI Reference](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/)
