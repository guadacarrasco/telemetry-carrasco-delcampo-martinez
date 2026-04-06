# Semana 4: S3, DynamoDB y EventBridge con CDK

## Objetivos
- Migrar de SAM a CDK para infraestructura como código
- Crear tablas de DynamoDB para datos de sesiones y pilotos
- Almacenar datos crudos en S3
- Configurar una regla de EventBridge para ejecutar Lambda de forma programada
- Implementar el patrón repositorio para acceso a datos

## Herramientas
- AWS CDK (Python), DynamoDB, S3, EventBridge

## Actividad
1. Crear stacks de CDK para almacenamiento de datos (tablas DynamoDB + bucket S3)
2. Implementar clases repositorio para operaciones con DynamoDB y S3
3. Conectar una regla de EventBridge para disparar el Lambda de ingesta
4. Almacenar los datos ingestados tanto en S3 (crudo) como en DynamoDB (parseado)

## Pasos

### 1. Configurar el proyecto CDK
```bash
cd infra/cdk
pip install aws-cdk-lib constructs
```

### 2. Crear el DataStack
Define las tablas de DynamoDB:
- `f1_sessions` — PK: session_key (Number)
- `f1_driver_stats` — PK: session_key (Number), SK: driver_number (Number)

Define el bucket S3:
- `f1-raw-data` para almacenar las respuestas crudas de la API

### 3. Implementar los repositorios
Crea clases repositorio que usen boto3 para interactuar con DynamoDB y S3.
Usa la variable de entorno `AWS_ENDPOINT_URL` para compatibilidad con LocalStack.

### 4. Crear el MessagingStack
Define una regla de EventBridge que se ejecute cada 5 segundos (deshabilitada por defecto).

### 5. Probar con LocalStack
```bash
cd localstack && make start && make init
export AWS_ENDPOINT_URL=http://localhost:4566
python -c "from repositories.session_repo import SessionRepository; print('OK')"
```

## Conceptos clave
- **CDK vs SAM**: CDK usa lenguajes de programación reales, SAM usa plantillas YAML
- **CDK Stacks**: Agrupación lógica de recursos
- **Patrón Repositorio**: Capa de abstracción sobre el acceso a datos
- **EventBridge Rules**: Disparadores basados en horario o patrones de eventos
- **DynamoDB Partition/Sort Keys**: Patrones de acceso eficiente a datos
