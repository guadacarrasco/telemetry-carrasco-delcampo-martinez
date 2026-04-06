# Semana 1: Fundamentos de Lambda, IAM y Variables de Entorno

## Objetivos
- Comprender los fundamentos de AWS Lambda (handler, event, context)
- Crear una función Lambda que llame a la API de OpenF1
- Configurar roles de IAM y variables de entorno
- Desplegar usando SAM CLI

## Herramientas
- AWS Console/CLI, Python 3.12, boto3, SAM CLI

## Actividad
Crea una función Lambda `start_simulation` que:
1. Lea un `SESSION_KEY` desde las variables de entorno
2. Llame a la API de OpenF1 para obtener datos de sesión
3. Retorne la información de la sesión como JSON

## Pasos

### 1. Revisa el código inicial
Observa `starter/handler.py` — contiene un esqueleto de handler Lambda con comentarios TODO.

### 2. Implementa el handler
- Usa la librería `requests` para llamar a `https://api.openf1.org/v1/sessions?session_key={SESSION_KEY}`
- Parsea la respuesta y retorna la información de la sesión
- Maneja errores (fallos en la API, variables de entorno faltantes)

### 3. Crea el template de SAM
- Define un recurso `AWS::Serverless::Function`
- Configura `Runtime: python3.12`, `Timeout: 30`, `MemorySize: 256`
- Agrega la variable de entorno `SESSION_KEY`
- Configura un rol de IAM con permisos básicos de ejecución Lambda

### 4. Prueba localmente
```bash
sam local invoke StartSimulation --env-vars env.json
```

### 5. Despliega
```bash
sam build
sam deploy --guided
```

## Resultados esperados
- Lambda retorna JSON con session_key, session_name, circuit y fechas
- Los logs de CloudWatch muestran salida estructurada
- La función se ejecuta en menos de 5 segundos

## Conceptos clave
- **Lambda Handler**: `def handler(event, context)` — punto de entrada para todas las invocaciones
- **Event**: Datos de entrada (provenientes de API Gateway, SQS, EventBridge, etc.)
- **Context**: Información del runtime (nombre de la función, memoria, tiempo restante)
- **IAM Role**: Permisos que tiene la función Lambda para acceder a servicios de AWS
- **Variables de entorno**: Valores de configuración inyectados en tiempo de ejecución

## Recursos
- [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/latest/dg/)
- [OpenF1 API Documentation](https://openf1.org/)
- [SAM CLI Reference](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/)
