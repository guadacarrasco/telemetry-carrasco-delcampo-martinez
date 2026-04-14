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
cd activities/part-1/week-04-data-storage/solution
pip install -r requirements.txt
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
Define una regla de EventBridge programada (en la solución se usa `rate(1 minuto)`, el mínimo que admite EventBridge; deshabilitada por defecto).

### 5. Probar con LocalStack
```bash
cd localstack && make start && make init
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
cd ../activities/part-1/week-04-data-storage/solution && PYTHONPATH=. python -c "from repositories.session_repo import SessionRepository; print('OK')"
```
Para `make init`, instalá [aws-cdk-local](https://github.com/localstack/aws-cdk-local) (`pip install aws-cdk-local`) para tener `cdklocal` en el PATH. Si CDK/jsii no puede escribir en el caché por defecto, usá por ejemplo `export JSII_PACKAGE_CACHE=$PWD/.jsii-package-cache` antes del deploy.

## Conceptos clave
- **CDK vs SAM**: CDK usa lenguajes de programación reales, SAM usa plantillas YAML
- **CDK Stacks**: Agrupación lógica de recursos
- **Patrón Repositorio**: Capa de abstracción sobre el acceso a datos
- **EventBridge Rules**: Disparadores basados en horario o patrones de eventos
- **DynamoDB Partition/Sort Keys**: Patrones de acceso eficiente a datos
